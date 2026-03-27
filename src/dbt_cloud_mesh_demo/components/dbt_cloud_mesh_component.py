"""Custom dbt Cloud component that handles dbt mesh cross-project references.

When a dbt Cloud project includes models from an external package (via dbt mesh),
this component creates external asset specs for those models so that Dagster
can track lineage across project boundaries without duplicating assets.

The built-in polling sensor from DbtCloudComponent observes dbt Cloud runs
triggered outside of Dagster and emits materialization events automatically.
"""

import json
from collections.abc import Mapping
from functools import cached_property
from pathlib import Path
from typing import Annotated, Any

import dagster as dg
from dagster.components.resolved.model import Resolver
from dagster_dbt import DagsterDbtTranslator, DbtCloudComponent
from pydantic import Field


class _GroupOverrideTranslator(DagsterDbtTranslator):
    """A translator that applies group overrides based on resource type or fqn segments."""

    def __init__(
        self, group_overrides: dict[str, str], settings: Any | None = None
    ):
        super().__init__(settings=settings)
        self._group_overrides = group_overrides

    def get_group_name(
        self, dbt_resource_props: Mapping[str, Any]
    ) -> str | None:
        if self._group_overrides:
            # Check resource_type first (e.g. "seed", "model", "test")
            resource_type = dbt_resource_props.get("resource_type", "")
            if resource_type in self._group_overrides:
                return self._group_overrides[resource_type]

            # Then check each fqn segment (e.g. "staging", "silver")
            for segment in dbt_resource_props.get("fqn", []):
                if segment in self._group_overrides:
                    return self._group_overrides[segment]

        # Fall back to default translator behavior (uses dbt group if set)
        return super().get_group_name(dbt_resource_props)


class DbtCloudMeshComponent(DbtCloudComponent):
    """A DbtCloudComponent that generates external assets for cross-project dbt mesh references.

    Extends the standard DbtCloudComponent with:
    - `external_packages`: Creates external asset specs for models from other dbt projects,
      preserving cross-project lineage without duplicating assets.
    - `group_overrides`: Maps dbt resource types or fqn path segments to Dagster group names,
      allowing group assignment from YAML even when the dbt project doesn't set groups.

    The built-in `create_sensor` (default: true) polls dbt Cloud for runs triggered
    externally and emits materialization events into Dagster's asset graph.
    """

    external_packages: Annotated[
        dict[str, dict[str, Any]],
        Resolver.default(
            description=(
                "Map of external package names to their config. "
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
            return _GroupOverrideTranslator(
                group_overrides=self.group_overrides, settings=settings
            )
        return DagsterDbtTranslator(settings)

    def build_defs_from_state(
        self, context: dg.ComponentLoadContext, state_path: Path | None
    ) -> dg.Definitions:
        base_defs = super().build_defs_from_state(context, state_path)

        if not self.external_packages or state_path is None:
            return base_defs

        # Read the manifest from state to find external package models
        from dagster._serdes import deserialize_value

        workspace_data = deserialize_value(state_path.read_text())
        manifest = workspace_data.manifest

        if not manifest:
            return base_defs

        if isinstance(manifest, (str, Path)):
            manifest = json.loads(Path(manifest).read_text())

        external_assets = self._create_external_assets(manifest)

        if external_assets:
            return dg.Definitions(
                assets=[*base_defs.assets, *external_assets],
                resources=base_defs.resources,
                schedules=base_defs.schedules,
                sensors=base_defs.sensors,
            )

        return base_defs

    def _create_external_assets(
        self, manifest: Mapping[str, Any]
    ) -> list[dg.AssetSpec]:
        external_specs: list[dg.AssetSpec] = []

        for node_id, node_info in manifest.get("nodes", {}).items():
            package_name = node_info.get("package_name", "")

            if package_name not in self.external_packages:
                continue

            if node_info.get("resource_type") != "model":
                continue

            # Only expose public models (respecting dbt mesh access modifiers)
            if node_info.get("access", "protected") != "public":
                continue

            package_config = self.external_packages[package_name]
            key_prefix = package_config.get("key_prefix", [package_name])
            group_name = package_config.get("group_name", package_name)

            asset_key = dg.AssetKey([*key_prefix, node_info["name"]])

            external_specs.append(
                dg.AssetSpec(
                    key=asset_key,
                    group_name=group_name,
                    description=node_info.get(
                        "description",
                        f"External model from {package_name}",
                    ),
                    metadata={
                        "dbt/package": package_name,
                        "dbt/original_file_path": node_info.get(
                            "original_file_path", ""
                        ),
                    },
                )
            )

        return external_specs
