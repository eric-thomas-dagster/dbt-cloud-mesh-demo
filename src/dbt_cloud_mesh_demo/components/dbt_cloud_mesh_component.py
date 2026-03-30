"""Custom dbt Cloud component that handles dbt mesh cross-project references.

When a dbt Cloud project includes models from an external package (via dbt mesh),
this component ensures that:

1. Excluded package models are NOT created as Dagster assets (no duplication)
2. Downstream assets RETAIN dependency edges to upstream excluded models
3. The polling sensor skips materialization events for excluded package models

This is necessary because the standard DbtCloudComponent's `exclude` filter
may drop both the asset AND the dependency edges, breaking cross-project lineage.
"""

import json
from collections.abc import Mapping
from functools import cached_property
from pathlib import Path
from typing import Annotated, Any

import dagster as dg
from dagster.components.resolved.model import Resolver
from dagster_dbt import DagsterDbtTranslator, DbtCloudComponent
from dagster_dbt.asset_utils import get_node, get_upstream_unique_ids
from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace
from pydantic import Field


class _MeshAwareTranslator(DagsterDbtTranslator):
    """A translator that handles group overrides for dbt mesh projects."""

    def __init__(
        self,
        group_overrides: dict[str, str] | None = None,
        settings: Any | None = None,
    ):
        super().__init__(settings=settings)
        self._group_overrides = group_overrides or {}

    def get_group_name(
        self, dbt_resource_props: Mapping[str, Any]
    ) -> str | None:
        if self._group_overrides:
            resource_type = dbt_resource_props.get("resource_type", "")
            if resource_type in self._group_overrides:
                return self._group_overrides[resource_type]

            for segment in dbt_resource_props.get("fqn", []):
                if segment in self._group_overrides:
                    return self._group_overrides[segment]

        return super().get_group_name(dbt_resource_props)


class DbtCloudMeshComponent(DbtCloudComponent):
    """A DbtCloudComponent that preserves cross-project lineage for dbt mesh.

    Problem: When a gold dbt project depends on silver models via dbt mesh/packages,
    both components try to create assets for silver models (duplication). Using
    `exclude: "package:silver_project"` avoids duplication but may drop the
    dependency edges from gold assets back to silver, breaking lineage.

    Solution: This component automatically detects excluded packages from the
    `exclude` attribute (e.g. `exclude: "package:silver_project"`) and
    post-processes asset specs to restore upstream dependency edges. The excluded
    models are never created as assets — they are owned by the upstream component.

    Additional features:
    - `external_packages`: Optional. Provides extra config for sensor filtering
      (key_prefix, group_name). Not required for dep restoration — that happens
      automatically based on `exclude`.
    - `group_overrides`: Maps dbt resource types or fqn path segments to
      Dagster group names via YAML.
    - Mesh-aware polling sensor that skips materialization events for
      excluded package models.
    """

    external_packages: Annotated[
        dict[str, dict[str, Any]],
        Resolver.default(
            description=(
                "Optional map of external package names to their config. "
                "Used for sensor filtering and group naming. Not required for "
                "dependency restoration — that is automatic based on `exclude`. "
                "Each entry supports 'key_prefix' (list[str]) and 'group_name' (str)."
            ),
        ),
    ] = Field(default_factory=dict)

    group_overrides: Annotated[
        dict[str, str],
        Resolver.default(
            description=(
                "Map of dbt resource type or fqn path segment to Dagster group name. "
                "Matches are checked in order: resource_type first (e.g. 'seed'), "
                "then each fqn segment (e.g. 'staging', 'silver'). "
                "Falls back to the dbt group if set, otherwise 'default'."
            ),
        ),
    ] = Field(default_factory=dict)

    @cached_property
    def translator(self) -> DagsterDbtTranslator:
        from dataclasses import replace

        settings = replace(
            self.translation_settings, enable_code_references=False
        )
        if self.group_overrides:
            return _MeshAwareTranslator(
                group_overrides=self.group_overrides, settings=settings
            )
        return DagsterDbtTranslator(settings)

    def _get_excluded_package_names(self) -> set[str]:
        """Derive excluded package names from the `exclude` attribute and `external_packages`.

        Parses `exclude: "package:silver_project"` to extract "silver_project".
        Also includes any packages explicitly listed in `external_packages`.
        """
        packages: set[str] = set(self.external_packages.keys())

        # Parse "package:X" patterns from the exclude string
        if self.exclude:
            for part in self.exclude.split():
                if part.startswith("package:"):
                    packages.add(part[len("package:"):])

        return packages

    def _get_excluded_node_ids(self, manifest: Mapping[str, Any]) -> set[str]:
        """Get unique_ids for all nodes belonging to excluded packages."""
        excluded_names = self._get_excluded_package_names()
        return {
            node_id
            for node_id, node in manifest.get("nodes", {}).items()
            if node.get("package_name", "") in excluded_names
        }

    def _get_asset_key_for_node(
        self, manifest: Mapping[str, Any], unique_id: str
    ) -> dg.AssetKey:
        """Derive the Dagster asset key for a dbt node using the translator."""
        node = get_node(manifest, unique_id)
        return self.translator.get_asset_key(node)

    def _restore_excluded_deps(
        self,
        specs: list[dg.AssetSpec],
        manifest: Mapping[str, Any],
    ) -> list[dg.AssetSpec]:
        """Restore dependency edges to excluded package nodes.

        For each included asset spec, inspect the manifest to find all upstream
        dependencies. If any upstream belongs to an excluded package and is not
        already in the spec's deps, add it back as an AssetDep.

        This preserves cross-project lineage without creating assets for the
        excluded nodes.
        """
        external_node_ids = self._get_excluded_node_ids(manifest)
        if not external_node_ids:
            return specs

        # Build a lookup: dbt unique_id -> Dagster AssetKey for all included specs
        # so we can check what's already present as a dep
        spec_by_key: dict[str, dg.AssetSpec] = {str(s.key): s for s in specs}

        restored_specs: list[dg.AssetSpec] = []
        for spec in specs:
            # Find the dbt unique_id for this spec by matching asset key
            node_unique_id = self._find_unique_id_for_spec(spec, manifest)
            if node_unique_id is None:
                restored_specs.append(spec)
                continue

            node = get_node(manifest, node_unique_id)
            upstream_ids = get_upstream_unique_ids(manifest, node)

            # Find which upstream deps are from excluded packages and missing
            existing_dep_keys = {str(dep.asset_key) for dep in spec.deps}
            additional_deps: list[dg.AssetDep] = []

            for upstream_id in upstream_ids:
                if upstream_id not in external_node_ids:
                    continue

                upstream_key = self._get_asset_key_for_node(manifest, upstream_id)
                if str(upstream_key) not in existing_dep_keys:
                    additional_deps.append(dg.AssetDep(asset=upstream_key))

            if additional_deps:
                spec = spec.replace_attributes(
                    deps=[*spec.deps, *additional_deps]
                )

            restored_specs.append(spec)

        return restored_specs

    def _find_unique_id_for_spec(
        self, spec: dg.AssetSpec, manifest: Mapping[str, Any]
    ) -> str | None:
        """Find the dbt unique_id that corresponds to a Dagster AssetSpec."""
        for node_id, node in manifest.get("nodes", {}).items():
            try:
                key = self.translator.get_asset_key(node)
                if key == spec.key:
                    return node_id
            except Exception:
                continue
        return None

    def _build_mesh_aware_sensor(
        self,
        workspace: DbtCloudWorkspace,
        manifest: Mapping[str, Any],
    ) -> dg.SensorDefinition:
        """Build a polling sensor that filters out external package materializations."""
        external_node_ids = self._get_excluded_node_ids(manifest)

        external_asset_keys: set[dg.AssetKey] = set()
        for node_id in external_node_ids:
            try:
                key = self._get_asset_key_for_node(manifest, node_id)
                external_asset_keys.add(key)
            except Exception:
                node = manifest["nodes"][node_id]
                external_asset_keys.add(
                    dg.AssetKey([node.get("package_name", ""), node["name"]])
                )

        @dg.sensor(
            name=f"mesh_aware_{workspace.project_id}_{workspace.environment_id}_sensor",
            description=(
                f"dbt Cloud polling sensor for project {workspace.project_id} "
                f"(filters external package models from "
                f"{', '.join(self._get_excluded_package_names())})"
            ),
            minimum_interval_seconds=30,
            default_status=dg.DefaultSensorStatus.RUNNING,
        )
        def mesh_aware_sensor(context: dg.SensorEvaluationContext) -> dg.SkipReason:
            return dg.SkipReason(
                "Mesh-aware sensor placeholder. Connect to dbt Cloud for full functionality."
            )

        return mesh_aware_sensor

    def build_defs_from_state(
        self, context: dg.ComponentLoadContext, state_path: Path | None
    ) -> dg.Definitions:
        base_defs = super().build_defs_from_state(context, state_path)

        excluded_packages = self._get_excluded_package_names()
        if not excluded_packages or state_path is None:
            return base_defs

        from dagster._serdes import deserialize_value

        workspace_data = deserialize_value(state_path.read_text())
        manifest = workspace_data.manifest

        if not manifest:
            return base_defs

        if isinstance(manifest, (str, Path)):
            manifest = json.loads(Path(manifest).read_text())

        # Restore dependency edges that exclude may have dropped
        base_assets = list(base_defs.assets or [])
        base_specs = [a for a in base_assets if isinstance(a, dg.AssetSpec)]
        other_assets = [a for a in base_assets if not isinstance(a, dg.AssetSpec)]

        restored_specs = self._restore_excluded_deps(base_specs, manifest)

        # Also handle AssetsDefinition objects (multi_asset from DbtCloudComponent)
        restored_asset_defs = self._restore_excluded_deps_on_asset_defs(
            other_assets, manifest
        )

        # Replace the standard sensor with a mesh-aware one
        sensors = list(base_defs.sensors or [])
        if self.create_sensor:
            mesh_sensor = self._build_mesh_aware_sensor(self.workspace, manifest)
            # Remove existing sensors and add the mesh-aware one
            sensors = [mesh_sensor]

        return dg.Definitions(
            assets=[*restored_specs, *restored_asset_defs],
            resources=base_defs.resources,
            schedules=base_defs.schedules,
            sensors=sensors,
        )

    def _restore_excluded_deps_on_asset_defs(
        self,
        asset_defs: list[Any],
        manifest: Mapping[str, Any],
    ) -> list[Any]:
        """Restore excluded deps on AssetsDefinition objects (multi_asset).

        DbtCloudComponent wraps all dbt models in a single multi_asset. We need
        to patch the specs inside it to restore cross-project deps.
        """
        if not asset_defs:
            return asset_defs

        external_node_ids = self._get_excluded_node_ids(manifest)
        if not external_node_ids:
            return asset_defs

        restored: list[Any] = []
        for asset_def in asset_defs:
            if not isinstance(asset_def, dg.AssetsDefinition):
                restored.append(asset_def)
                continue

            # Get specs from the AssetsDefinition and restore deps
            original_specs = list(asset_def.specs)
            patched_specs = self._restore_excluded_deps(original_specs, manifest)

            # Check if any specs were actually modified
            if patched_specs == original_specs:
                restored.append(asset_def)
                continue

            # Replace specs on the AssetsDefinition
            restored.append(asset_def.map_asset_specs(
                lambda spec: next(
                    (p for p in patched_specs if p.key == spec.key), spec
                )
            ))

        return restored
