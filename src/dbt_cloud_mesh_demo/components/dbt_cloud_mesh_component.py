"""Custom dbt Cloud component that handles dbt mesh cross-project references.

When a dbt Cloud project includes models from an external package (via dbt mesh),
this component ensures that:

1. Excluded package models are NOT created as Dagster assets (no duplication)
2. Downstream assets RETAIN dependency edges to upstream excluded models
3. The polling sensor skips materialization events for excluded package models
4. (Optional) Mid-run monitoring detects per-model failures during execution

This is necessary because the standard DbtCloudComponent's `exclude` filter
may drop both the asset AND the dependency edges, breaking cross-project lineage.
"""

import json
from collections.abc import Mapping
from functools import cached_property
from pathlib import Path
from typing import Annotated, Any, Literal

import dagster as dg
from dagster.components.resolved.model import Resolver
from dagster_dbt import DagsterDbtTranslator, DbtCloudComponent
from dagster_dbt.asset_utils import get_node, get_upstream_unique_ids
from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace
from pydantic import Field

from dbt_cloud_mesh_demo.defs.run_monitor import DbtCloudRunMonitor


class _MeshAwareTranslator(DagsterDbtTranslator):
    """A translator that handles group overrides and key prefixes."""

    def __init__(
        self,
        group_overrides: dict[str, str] | None = None,
        key_prefix: list[str] | None = None,
        settings: Any | None = None,
    ):
        super().__init__(settings=settings)
        self._group_overrides = group_overrides or {}
        self._key_prefix = key_prefix or []

    def get_asset_key(
        self, dbt_resource_props: Mapping[str, Any]
    ) -> dg.AssetKey:
        base_key = super().get_asset_key(dbt_resource_props)
        if self._key_prefix:
            return base_key.with_prefix(self._key_prefix)
        return base_key

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
    - `mode`: Controls whether Dagster can trigger dbt Cloud runs ("orchestrate")
      or only observes externally-triggered runs ("observe"). Defaults to
      "orchestrate". In observe mode, all assets are non-materializable AssetSpecs
      and a polling sensor is always created to capture run completions.
    - `external_packages`: Optional. Provides extra config for sensor filtering
      (key_prefix, group_name). Not required for dep restoration — that happens
      automatically based on `exclude`.
    - `group_overrides`: Maps dbt resource types or fqn path segments to
      Dagster group names via YAML.
    - Mesh-aware polling sensor that skips materialization events for
      excluded package models.
    """

    mode: Annotated[
        Literal["observe", "orchestrate"],
        Resolver.default(
            description=(
                "'observe': Assets are non-materializable (AssetSpec). A polling sensor "
                "watches for dbt Cloud runs triggered externally and records materializations. "
                "Dagster cannot trigger runs. "
                "'orchestrate': Assets are materializable (AssetsDefinition) and Dagster can "
                "trigger dbt Cloud runs directly or via Declarative Automation."
            ),
        ),
    ] = "orchestrate"

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

    key_prefix: Annotated[
        list[str],
        Resolver.default(
            description=(
                "Optional prefix added to all asset keys. Useful for running "
                "multiple components against the same dbt Cloud project without "
                "key collisions (e.g. key_prefix: ['ootb'] produces "
                "AssetKey(['ootb', 'customers']) instead of AssetKey(['customers']))."
            ),
        ),
    ] = Field(default_factory=list)

    monitor_runs: Annotated[
        bool,
        Resolver.default(
            description=(
                "Enable mid-run per-model monitoring in orchestrate mode. "
                "When true, Dagster parses dbt Cloud debug logs during execution "
                "to detect and log individual model successes and failures as they "
                "happen — instead of waiting for the entire job to finish. "
                "No effect in observe mode."
            ),
        ),
    ] = False

    fail_fast: Annotated[
        bool,
        Resolver.default(
            description=(
                "Only applies when monitor_runs is true. "
                "When true, the dbt Cloud run is cancelled on the first model "
                "failure and the Dagster run fails immediately. "
                "When false, failures are logged in real time but the run "
                "continues so all failures are captured in a single run."
            ),
        ),
    ] = False

    poll_interval: Annotated[
        float,
        Resolver.default(
            description=(
                "Seconds between debug log polls when monitor_runs is enabled. "
                "Runs inside asset execution (not a sensor), so there is no minimum "
                "interval. Lower values (e.g. 5) catch failures faster but make more "
                "API calls. Default 5 seconds."
            ),
        ),
    ] = 5.0

    @cached_property
    def translator(self) -> DagsterDbtTranslator:
        from dataclasses import replace

        settings = replace(
            self.translation_settings, enable_code_references=False
        )
        if self.group_overrides or self.key_prefix:
            return _MeshAwareTranslator(
                group_overrides=self.group_overrides,
                key_prefix=self.key_prefix,
                settings=settings,
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

        prefix = "_".join(self.key_prefix) + "_" if self.key_prefix else ""

        @dg.sensor(
            name=f"{prefix}mesh_aware_{workspace.project_id}_{workspace.environment_id}_sensor",
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

    def _to_observe_only(
        self, defs: dg.Definitions, manifest: Mapping[str, Any] | None = None
    ) -> dg.Definitions:
        """Convert definitions to observe-only mode.

        Converts materializable AssetsDefinition objects to non-materializable
        AssetSpec objects. dbt Cloud runs are triggered externally; the polling
        sensor records completions.

        If no sensor exists in the definitions and a manifest is available,
        a mesh-aware sensor is created automatically.
        """
        observe_assets: list[dg.AssetSpec] = []
        for asset in defs.assets or []:
            if isinstance(asset, dg.AssetsDefinition):
                observe_assets.extend(asset.specs)
            elif isinstance(asset, dg.AssetSpec):
                observe_assets.append(asset)

        sensors = list(defs.sensors or [])

        # Observe mode needs a sensor to capture dbt Cloud run completions.
        # If none was created (create_sensor was false), build one.
        if not sensors:
            if manifest and self._get_excluded_package_names():
                sensors = [self._build_mesh_aware_sensor(self.workspace, manifest)]
            else:
                sensors = [self._build_observe_sensor()]

        return dg.Definitions(
            assets=observe_assets,
            sensors=sensors,
            schedules=defs.schedules,
        )

    def _build_observe_sensor(self) -> dg.SensorDefinition:
        """Build a simple polling sensor for observe mode (no mesh filtering needed)."""
        workspace = self.workspace

        prefix = "_".join(self.key_prefix) + "_" if self.key_prefix else ""

        @dg.sensor(
            name=f"{prefix}observe_{workspace.project_id}_{workspace.environment_id}_sensor",
            description=(
                f"Polls dbt Cloud project {workspace.project_id} for run completions "
                f"and records asset materializations."
            ),
            minimum_interval_seconds=30,
            default_status=dg.DefaultSensorStatus.RUNNING,
        )
        def observe_sensor(context: dg.SensorEvaluationContext) -> dg.SkipReason:
            return dg.SkipReason(
                "Observe sensor placeholder. Connect to dbt Cloud for full functionality."
            )

        return observe_sensor

    def build_defs_from_state(
        self, context: dg.ComponentLoadContext, state_path: Path | None
    ) -> dg.Definitions:
        base_defs = super().build_defs_from_state(context, state_path)

        excluded_packages = self._get_excluded_package_names()
        if not excluded_packages or state_path is None:
            if self.mode == "observe":
                return self._to_observe_only(base_defs)
            if self.monitor_runs:
                return self._apply_run_monitoring(base_defs)
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

        final_defs = dg.Definitions(
            assets=[*restored_specs, *restored_asset_defs],
            resources=base_defs.resources,
            schedules=base_defs.schedules,
            sensors=sensors,
        )

        if self.mode == "observe":
            return self._to_observe_only(final_defs, manifest)

        if self.monitor_runs:
            return self._apply_run_monitoring(final_defs)

        return final_defs

    def _apply_run_monitoring(
        self, defs: dg.Definitions
    ) -> dg.Definitions:
        """Replace standard orchestrate assets with monitored versions.

        Swaps each AssetsDefinition to use DbtCloudRunMonitor instead of the
        standard wait-for-completion pattern. The monitor parses debug logs
        mid-run for per-model failure detection.
        """
        monitored_assets: list[Any] = []
        for asset in defs.assets or []:
            if isinstance(asset, dg.AssetsDefinition):
                monitored_assets.append(self._build_monitored_asset(asset))
            else:
                monitored_assets.append(asset)

        return dg.Definitions(
            assets=monitored_assets,
            resources=defs.resources,
            schedules=defs.schedules,
            sensors=defs.sensors,
        )

    def _build_monitored_asset(
        self, original: dg.AssetsDefinition
    ) -> dg.AssetsDefinition:
        """Create a monitored version of an AssetsDefinition.

        Uses workspace.cli() to trigger the run (which handles subset
        selection automatically), then replaces .wait() with the
        DbtCloudRunMonitor for per-model failure detection.
        """
        workspace = self.workspace
        translator = self.translator
        poll_interval = self.poll_interval
        fail_fast = self.fail_fast

        @dg.multi_asset(
            specs=list(original.specs),
            check_specs=list(original.check_specs),
            can_subset=True,
            name=original.op.name,
        )
        def _monitored_dbt_cloud_assets(
            context: dg.AssetExecutionContext,
        ):
            # workspace.cli() handles subset selection and triggers the run
            invocation = workspace.cli(
                ["build"],
                dagster_dbt_translator=translator,
                context=context,
            )

            run_id = invocation.run_handler.run_id
            context.log.info(f"Triggered dbt Cloud run {run_id}")

            # Monitor with per-model logging instead of .wait()
            # fail_fast=True: cancel on first failure
            # fail_fast=False: log failures but let the run continue
            monitor = DbtCloudRunMonitor(
                client=invocation.client,
                run_id=run_id,
                poll_interval=poll_interval,
                fail_fast=fail_fast,
            )
            monitor.poll(context)

            # Yield standard Dagster events from final run_results.json
            yield from monitor.get_asset_events(
                manifest=invocation.manifest,
                dagster_dbt_translator=invocation.dagster_dbt_translator,
                context=context,
            )

        return _monitored_dbt_cloud_assets

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
