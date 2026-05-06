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
from datetime import timedelta
from functools import cached_property
from pathlib import Path
from typing import Annotated, Any, Literal

import dagster as dg
from dagster.components.resolved.model import Resolver
from dagster_dbt import DagsterDbtTranslator, DbtCloudComponent
from dagster_dbt.asset_utils import build_dbt_specs, get_node, get_upstream_unique_ids
from dagster_dbt.cloud_v2.resources import DbtCloudWorkspace
from dagster_dbt.cloud_v2.run_handler import DbtCloudJobRunResults
from dagster_dbt.cloud_v2.types import DbtCloudJobRunStatusType, DbtCloudRun
from dagster_dbt.compat import REFABLE_NODE_TYPES, NodeStatus
from dagster_shared.serdes import deserialize_value, serialize_value
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

    materialize_views: Annotated[
        bool,
        Resolver.default(
            description=(
                "Whether dbt view models should be materializable. When false, "
                "views are treated as virtual assets — they appear in the lineage "
                "graph but are never materialized, saving compute. Since a view "
                "always reflects the current state of its upstream data, there is "
                "nothing to refresh. Default true (all models materializable)."
            ),
        ),
    ] = True

    asset_granularity: Annotated[
        Literal["model", "job"],
        Resolver.default(
            description=(
                "'model': Each dbt model is a separate Dagster asset with full "
                "lineage (default). "
                "'job': The entire dbt Cloud workspace is a single Dagster asset, "
                "like how Fivetran/Airbyte connectors appear as one asset. "
                "Internal model lineage stays in dbt Cloud. Simpler for teams "
                "that don't need per-model orchestration in Dagster."
            ),
        ),
    ] = "model"

    @cached_property
    def translator(self) -> DagsterDbtTranslator:
        from dataclasses import replace

        settings = replace(
            self.translation_settings,
            enable_code_references=False,
            enable_dbt_views_as_virtual_assets=not self.materialize_views,
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
        """Build a polling sensor that filters external packages and emits check evaluations.

        Unlike the OOTB sensor which only emits AssetMaterialization for successes,
        this sensor also emits AssetCheckEvaluation for every model result — so
        failed models show a degraded check in the Dagster UI.
        """
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
        translator = self.translator
        captured_manifest = manifest

        @dg.sensor(
            name=f"{prefix}mesh_aware_{workspace.project_id}_{workspace.environment_id}_sensor",
            description=(
                f"dbt Cloud polling sensor for project {workspace.project_id}. "
                f"Emits materializations for successes and check evaluations for "
                f"all model results (including failures)."
            ),
            minimum_interval_seconds=30,
            default_status=dg.DefaultSensorStatus.RUNNING,
        )
        def mesh_aware_sensor(context: dg.SensorEvaluationContext) -> dg.SensorResult:
            from dagster._time import get_current_datetime

            # Cursor management — track last seen timestamp
            cursor_timestamp = float(context.cursor) if context.cursor else None
            current_time = get_current_datetime()

            if cursor_timestamp is None:
                # First run: look back 60 seconds
                lower_bound = (current_time - timedelta(seconds=60)).timestamp()
            else:
                lower_bound = cursor_timestamp

            upper_bound = current_time.timestamp()

            client = workspace.get_client()
            workspace_data = workspace.get_or_fetch_workspace_data()

            from dagster._time import datetime_from_timestamp

            runs, _total = client.get_runs_batch(
                project_id=workspace.project_id,
                environment_id=workspace.environment_id,
                finished_at_lower_bound=datetime_from_timestamp(lower_bound),
                finished_at_upper_bound=datetime_from_timestamp(upper_bound),
                offset=0,
            )

            all_events: list[dg.AssetMaterialization | dg.AssetCheckEvaluation] = []

            for run_details in runs:
                run = DbtCloudRun.from_run_details(run_details)

                # Skip Dagster-triggered runs
                if run.job_definition_id == workspace_data.adhoc_job_id:
                    continue

                run_artifacts = client.list_run_artifacts(run_id=run.id)
                if "run_results.json" not in run_artifacts:
                    continue

                run_results_json = client.get_run_results_json(run_id=run.id)
                invocation_id = run_results_json.get("metadata", {}).get("invocation_id", "")

                for result in run_results_json.get("results", []):
                    unique_id = result.get("unique_id", "")
                    node = captured_manifest.get("nodes", {}).get(unique_id)
                    if not node:
                        continue

                    resource_type = node.get("resource_type", "")
                    if resource_type not in REFABLE_NODE_TYPES:
                        continue

                    asset_key = translator.get_asset_key(node)

                    # Filter out external package models
                    if asset_key in external_asset_keys:
                        continue

                    result_status = result.get("status", "")
                    exec_time = result.get("execution_time", 0.0)
                    message = result.get("message", "")

                    metadata = {
                        "unique_id": unique_id,
                        "invocation_id": invocation_id,
                        "execution_duration": exec_time,
                    }
                    if run.url:
                        metadata["run_url"] = dg.MetadataValue.url(run.url)

                    # Emit materialization for successful models
                    if result_status == NodeStatus.Success:
                        all_events.append(
                            dg.AssetMaterialization(
                                asset_key=asset_key,
                                metadata=metadata,
                            )
                        )

                    # Emit check evaluation for ALL models — success or failure.
                    # This makes failed models show a degraded check in the UI.
                    all_events.append(
                        dg.AssetCheckEvaluation(
                            asset_key=asset_key,
                            check_name="dbt_cloud_run_status",
                            passed=result_status == NodeStatus.Success,
                            metadata={
                                **metadata,
                                "status": result_status,
                                "message": message,
                            },
                            severity=dg.AssetCheckSeverity.ERROR,
                        )
                    )

            context.update_cursor(str(upper_bound))
            context.log.info(f"Emitting {len(all_events)} events from {len(runs)} runs")
            return dg.SensorResult(asset_events=all_events)

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
        """Build a polling sensor with check evaluations (no mesh filtering)."""
        workspace = self.workspace
        translator = self.translator

        prefix = "_".join(self.key_prefix) + "_" if self.key_prefix else ""

        @dg.sensor(
            name=f"{prefix}observe_{workspace.project_id}_{workspace.environment_id}_sensor",
            description=(
                f"Polls dbt Cloud project {workspace.project_id} for run completions. "
                f"Emits materializations for successes and check evaluations for all results."
            ),
            minimum_interval_seconds=30,
            default_status=dg.DefaultSensorStatus.RUNNING,
        )
        def observe_sensor(context: dg.SensorEvaluationContext) -> dg.SensorResult:
            from dagster._time import datetime_from_timestamp, get_current_datetime

            cursor_timestamp = float(context.cursor) if context.cursor else None
            current_time = get_current_datetime()
            lower_bound = cursor_timestamp or (current_time - timedelta(seconds=60)).timestamp()
            upper_bound = current_time.timestamp()

            client = workspace.get_client()
            workspace_data = workspace.get_or_fetch_workspace_data()

            runs, _total = client.get_runs_batch(
                project_id=workspace.project_id,
                environment_id=workspace.environment_id,
                finished_at_lower_bound=datetime_from_timestamp(lower_bound),
                finished_at_upper_bound=datetime_from_timestamp(upper_bound),
                offset=0,
            )

            all_events: list[dg.AssetMaterialization | dg.AssetCheckEvaluation] = []

            for run_details in runs:
                run = DbtCloudRun.from_run_details(run_details)
                if run.job_definition_id == workspace_data.adhoc_job_id:
                    continue

                run_artifacts = client.list_run_artifacts(run_id=run.id)
                if "run_results.json" not in run_artifacts:
                    continue

                run_results_json = client.get_run_results_json(run_id=run.id)
                invocation_id = run_results_json.get("metadata", {}).get("invocation_id", "")

                for result in run_results_json.get("results", []):
                    unique_id = result.get("unique_id", "")
                    node = workspace_data.manifest.get("nodes", {}).get(unique_id)
                    if not node:
                        continue
                    if node.get("resource_type", "") not in REFABLE_NODE_TYPES:
                        continue

                    asset_key = translator.get_asset_key(node)
                    result_status = result.get("status", "")
                    metadata = {
                        "unique_id": unique_id,
                        "invocation_id": invocation_id,
                        "execution_duration": result.get("execution_time", 0.0),
                    }
                    if run.url:
                        metadata["run_url"] = dg.MetadataValue.url(run.url)

                    if result_status == NodeStatus.Success:
                        all_events.append(
                            dg.AssetMaterialization(asset_key=asset_key, metadata=metadata)
                        )

                    all_events.append(
                        dg.AssetCheckEvaluation(
                            asset_key=asset_key,
                            check_name="dbt_cloud_run_status",
                            passed=result_status == NodeStatus.Success,
                            metadata={**metadata, "status": result_status, "message": result.get("message", "")},
                            severity=dg.AssetCheckSeverity.ERROR,
                        )
                    )

            context.update_cursor(str(upper_bound))
            context.log.info(f"Emitting {len(all_events)} events from {len(runs)} runs")
            return dg.SensorResult(asset_events=all_events)

        return observe_sensor

    def build_defs_from_state(
        self, context: dg.ComponentLoadContext, state_path: Path | None
    ) -> dg.Definitions:
        # Job-level granularity: one asset per workspace, no per-model lineage
        if self.asset_granularity == "job":
            return self._build_job_level_defs(state_path)

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

    def _build_job_level_defs(self, state_path: Path | None) -> dg.Definitions:
        """Build definitions with one asset per dbt Cloud job.

        Like Fivetran/Airbyte connectors, each dbt Cloud job is represented as
        a single Dagster asset instead of per-model assets. Internal model
        lineage stays in dbt Cloud. Jobs can be materialized (orchestrate)
        or observed (observe mode).
        """
        workspace = self.workspace
        prefix = self.key_prefix or []
        group_name = next(iter(self.group_overrides.values()), "dbt_cloud") if self.group_overrides else "dbt_cloud"

        # Get jobs from workspace data or API
        workspace_data = workspace.get_or_fetch_workspace_data()
        jobs = workspace_data.jobs or []

        # Filter out the adhoc job Dagster creates
        adhoc_job_id = workspace_data.adhoc_job_id
        user_jobs = [j for j in jobs if j.get("id") != adhoc_job_id]

        if not user_jobs:
            # No user-defined jobs — create a single asset for the workspace
            user_jobs = [{"id": adhoc_job_id, "name": "dbt_build"}]

        job_specs: list[dg.AssetSpec] = []
        job_id_by_key: dict[str, int] = {}

        for job in user_jobs:
            job_id = job.get("id")
            job_name = job.get("name", f"job_{job_id}")
            safe_name = job_name.lower().replace(" ", "_").replace("-", "_")
            asset_key = dg.AssetKey([*prefix, safe_name])

            job_specs.append(dg.AssetSpec(
                key=asset_key,
                group_name=group_name,
                description=job.get("description", f"dbt Cloud job: {job_name}"),
                metadata={
                    "dbt_cloud/job_id": job_id,
                    "dbt_cloud/job_name": job_name,
                    "dbt_cloud/project_id": workspace.project_id,
                    "dbt_cloud/environment_id": workspace.environment_id,
                },
                kinds={"dbt_cloud"},
            ))
            job_id_by_key[str(asset_key)] = job_id

        if self.mode == "observe":
            sensors = [self._build_job_level_sensor(workspace, job_id_by_key)]
            return dg.Definitions(assets=job_specs, sensors=sensors)

        # Orchestrate mode: materializable assets that trigger dbt Cloud jobs
        translator = self.translator

        @dg.multi_asset(
            specs=job_specs,
            can_subset=True,
            name=self._get_op_spec(f"dbt_cloud_jobs_{workspace.project_id}").name,
        )
        def _dbt_cloud_job_assets(context: dg.AssetExecutionContext):
            for key in context.selected_asset_keys:
                job_id = job_id_by_key.get(str(key))
                if not job_id:
                    continue

                client = workspace.get_client()
                run_details = client.trigger_job_run(job_id)
                run = DbtCloudRun.from_run_details(run_details)
                context.log.info(f"Triggered dbt Cloud job {job_id}, run {run.id}")
                if run.url:
                    context.log.info(f"dbt Cloud run URL: {run.url}")

                # Poll for completion
                client.poll_run(run.id)

                metadata: dict[str, Any] = {"dbt_cloud/run_id": run.id, "dbt_cloud/job_id": job_id}
                if run.url:
                    metadata["run_url"] = dg.MetadataValue.url(run.url)

                yield dg.Output(
                    value=None,
                    output_name=key.to_python_identifier(),
                    metadata=metadata,
                )

        sensors = []
        if self.create_sensor:
            sensors.append(self._build_job_level_sensor(workspace, job_id_by_key))

        return dg.Definitions(assets=[_dbt_cloud_job_assets], sensors=sensors)

    def _build_job_level_sensor(
        self, workspace: DbtCloudWorkspace, job_id_by_key: dict[str, int]
    ) -> dg.SensorDefinition:
        """Sensor that emits one materialization + check evaluation per completed job run."""
        prefix = "_".join(self.key_prefix) + "_" if self.key_prefix else ""

        # Reverse lookup: job_id → asset_key
        key_by_job_id: dict[int, dg.AssetKey] = {
            v: dg.AssetKey.from_user_string(k) for k, v in job_id_by_key.items()
        }

        @dg.sensor(
            name=f"{prefix}jobs_{workspace.project_id}_{workspace.environment_id}_sensor",
            description=f"Observes dbt Cloud job completions for project {workspace.project_id}",
            minimum_interval_seconds=30,
            default_status=dg.DefaultSensorStatus.RUNNING,
        )
        def _job_sensor(context: dg.SensorEvaluationContext) -> dg.SensorResult:
            from dagster._time import datetime_from_timestamp, get_current_datetime

            cursor_timestamp = float(context.cursor) if context.cursor else None
            current_time = get_current_datetime()
            lower_bound = cursor_timestamp or (current_time - timedelta(seconds=60)).timestamp()
            upper_bound = current_time.timestamp()

            client = workspace.get_client()
            workspace_data = workspace.get_or_fetch_workspace_data()

            runs, _total = client.get_runs_batch(
                project_id=workspace.project_id,
                environment_id=workspace.environment_id,
                finished_at_lower_bound=datetime_from_timestamp(lower_bound),
                finished_at_upper_bound=datetime_from_timestamp(upper_bound),
                offset=0,
            )

            events: list[dg.AssetMaterialization | dg.AssetCheckEvaluation] = []
            for run_details in runs:
                run = DbtCloudRun.from_run_details(run_details)
                if run.job_definition_id == workspace_data.adhoc_job_id:
                    continue

                asset_key = key_by_job_id.get(run.job_definition_id)
                if not asset_key:
                    continue

                is_success = run.status == DbtCloudJobRunStatusType.SUCCESS
                metadata: dict[str, Any] = {
                    "dbt_cloud/run_id": run.id,
                    "dbt_cloud/job_id": run.job_definition_id,
                }
                if run.url:
                    metadata["run_url"] = dg.MetadataValue.url(run.url)

                if is_success:
                    events.append(
                        dg.AssetMaterialization(asset_key=asset_key, metadata=metadata)
                    )

                events.append(
                    dg.AssetCheckEvaluation(
                        asset_key=asset_key,
                        check_name="dbt_cloud_run_status",
                        passed=is_success,
                        metadata={**metadata, "status": run.status.name if run.status else "UNKNOWN"},
                        severity=dg.AssetCheckSeverity.ERROR,
                    )
                )

            context.update_cursor(str(upper_bound))
            return dg.SensorResult(asset_events=events)

        return _job_sensor

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

            # Stream events mid-run — like dbt Core's .stream() but for dbt Cloud.
            # Yields Output events as models complete (from debug logs),
            # then remaining events from run_results.json on completion.
            # Raises Failure if any models errored.
            monitor = DbtCloudRunMonitor(
                client=invocation.client,
                run_id=run_id,
                poll_interval=poll_interval,
                fail_fast=fail_fast,
            )
            yield from monitor.stream(
                context=context,
                manifest=invocation.manifest,
                dagster_dbt_translator=invocation.dagster_dbt_translator,
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
