"""Enhanced dbt Cloud run monitor that surfaces model-level results mid-run.

The standard dagster-dbt polling loop (client.poll_run) only checks run status
and waits for the entire job to finish before reporting anything. This monitor
adds two layers of mid-run visibility:

1. **Debug log parsing** (per-model, real-time): Fetches the run's debug logs
   on each poll and parses dbt's structured log output for individual model
   success/failure lines. This gives per-model granularity DURING an active step.

2. **Artifact checking** (per-step): For multi-step jobs, checks if
   run_results.json has been updated as completed steps produce artifacts.

Limitation: dbt Cloud does not expose structured per-model results via any API
during an active run step. The Discovery API and run_results.json only update
after a run (or step) completes. Debug log parsing is the best available
workaround — it parses dbt's text output, which is consistent but not a
first-class API. For true real-time per-model streaming, use dbt Core via
Dagster's DbtCliResource instead.

Also note: dbt Cloud debug logs are truncated to the last ~1000 lines. For
very long runs with hundreds of models, early results may scroll out of the
log window. The artifact check (layer 2) catches everything once the step
finishes, so no results are permanently lost — just delayed.

Usage with @dbt_cloud_assets:

    @dbt_cloud_assets(workspace=dbt_cloud)
    def my_assets(context: AssetExecutionContext, dbt_cloud: DbtCloudWorkspace):
        client = dbt_cloud.get_client()
        workspace_data = dbt_cloud.get_or_fetch_workspace_data()

        run_details = client.trigger_job_run(workspace_data.adhoc_job_id)
        run = DbtCloudRun.from_run_details(run_details)

        monitor = DbtCloudRunMonitor(client=client, run_id=run.id, fail_fast=True)
        monitor.poll(context)

        yield from monitor.get_asset_events(workspace_data.manifest, context=context)
"""

import re
import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any, Union

from dagster import (
    AssetCheckEvaluation,
    AssetCheckResult,
    AssetExecutionContext,
    AssetMaterialization,
    Output,
)
from dagster_dbt.cloud_v2.client import DbtCloudWorkspaceClient
from dagster_dbt.cloud_v2.run_handler import DbtCloudJobRunResults
from dagster_dbt.cloud_v2.types import DbtCloudJobRunStatusType, DbtCloudRun
from dagster_dbt.dagster_dbt_translator import DagsterDbtTranslator

TERMINAL_STATUSES = {
    DbtCloudJobRunStatusType.SUCCESS,
    DbtCloudJobRunStatusType.ERROR,
    DbtCloudJobRunStatusType.CANCELLED,
}

FAILURE_STATUSES = {"error", "fail"}

# dbt logs model results like:
#   1 of 50 OK created sql_table_model schema.model_name .... [SELECT 1000 in 3.45s]
#   2 of 50 ERROR creating sql_table_model schema.model_name
# We want: the status (OK/ERROR) and the fully qualified model name.
# The model name always contains a dot (schema.model) and appears after the
# materialization type keyword.
_MODEL_RESULT_PATTERN = re.compile(
    r"(\d+)\s+of\s+(\d+)\s+"           # "N of M "
    r"(OK|ERROR)\s+"                    # status
    r"(?:created\s+|creating\s+)?"      # optional verb
    r"\S+\s+"                           # materialization type (sql_table_model, view, etc.)
    r"(\S+\.\S+)",                      # schema.model_name (must contain a dot)
    re.IGNORECASE,
)

# dbt logs test results like:
#   1 of 10 PASS unique_model_id ............ [PASS in 0.5s]
#   2 of 10 FAIL 3 not_null_model_col ....... [FAIL 3 in 0.8s]
#   3 of 10 WARN 1 accepted_values_model_col [WARN 1 in 0.3s]
_TEST_RESULT_PATTERN = re.compile(
    r"(\d+)\s+of\s+(\d+)\s+"           # "N of M "
    r"(PASS|FAIL|WARN|SKIP)\s+"         # status
    r"(?:\d+\s+)?"                      # optional failure count
    r"(\S+)",                           # test name
    re.IGNORECASE,
)

# Runtime errors reference the model explicitly
_RUNTIME_ERROR_PATTERN = re.compile(
    r"Runtime Error in model (\S+)",
    re.IGNORECASE,
)

# Compilation errors reference the node
_COMPILATION_ERROR_PATTERN = re.compile(
    r"Compilation Error in (?:model|test|snapshot|seed) (\S+)",
    re.IGNORECASE,
)

# Database errors reference the model
_DATABASE_ERROR_PATTERN = re.compile(
    r"Database Error in model (\S+)",
    re.IGNORECASE,
)


@dataclass
class ModelResult:
    """A single model/test result observed mid-run."""

    unique_id: str
    status: str
    execution_time: float
    message: str = ""
    source: str = ""  # "logs" or "artifacts" — for traceability


@dataclass
class DbtCloudRunMonitor:
    """Polls a dbt Cloud run and surfaces model-level results as they land.

    Adds two monitoring layers on top of the standard status poll:
    - Debug log parsing for per-model results during active steps
    - Artifact checking for per-step results in multi-step jobs

    Args:
        client: The dbt Cloud workspace client.
        run_id: The dbt Cloud run ID to monitor.
        poll_interval: Seconds between poll iterations. Default 5. Since this
            runs inside asset execution (not a sensor), there's no minimum
            interval restriction — poll as aggressively as needed. Shorter
            intervals reduce the chance of model results scrolling out of the
            ~1000-line debug log window before we parse them.
        fail_fast: If True, cancel the dbt Cloud run on first model failure
            and raise immediately.
    """

    client: DbtCloudWorkspaceClient
    run_id: int
    poll_interval: float = 5.0
    fail_fast: bool = False
    # Dedup sets — log-parsed names (schema.model) and artifact unique_ids
    # (model.package.model) are different namespaces, tracked separately.
    _seen_log_nodes: set[str] = field(default_factory=set)
    _seen_artifact_nodes: set[str] = field(default_factory=set)
    _all_results: list[ModelResult] = field(default_factory=list)

    def poll(
        self,
        context: AssetExecutionContext,
        timeout: float | None = None,
    ) -> None:
        """Poll until the run completes, logging model results as they appear.

        On each iteration:
        1. Check run status (+ fetch debug logs)
        2. Parse debug logs for per-model results (real-time)
        3. Check run_results.json artifacts for per-step results
        4. Fail fast if configured and failures detected

        Raises on run failure, cancellation, or timeout.
        """
        start_time = time.time()
        poll_count = 0
        log_parsing_available = False

        while True:
            poll_count += 1
            if timeout and (time.time() - start_time) > timeout:
                raise TimeoutError(
                    f"dbt Cloud run {self.run_id} timed out after {timeout}s"
                )

            # 1. Check run status
            #    Try to get debug logs in the same call. If it fails,
            #    fall back to a plain status check.
            run_details = self._get_run_details_safe(context)
            run = DbtCloudRun.from_run_details(run_details)
            status_name = run.status.name if run.status else "UNKNOWN"

            # On first poll, log what monitoring capabilities are available
            # so we know immediately if the steps API is working.
            if poll_count == 1:
                run_steps = run_details.get("run_steps", [])
                step_ids = [s.get("id") for s in run_steps]
                context.log.info(
                    f"Run {self.run_id} status: {status_name} | "
                    f"run_steps: {len(run_steps)} (ids: {step_ids}) | "
                    f"Response keys: {list(run_details.keys())}"
                )
                if run_steps:
                    # Try fetching debug logs from the latest step
                    latest_step = max(run_steps, key=lambda s: s.get("index", 0))
                    test_logs = self._fetch_step_debug_logs(latest_step["id"], context)
                    if test_logs:
                        log_parsing_available = True
                        context.log.info(
                            f"Debug logs available from step {latest_step['id']} "
                            f"({len(test_logs)} chars). Per-model monitoring active."
                        )
                    else:
                        context.log.warning(
                            "run_steps found but debug logs not yet available. "
                            "Will retry on subsequent polls (logs appear once "
                            "the step is RUNNING, not QUEUED/STARTING)."
                        )
                else:
                    context.log.warning(
                        "No run_steps in API response yet. "
                        "Will retry — steps appear once the run starts."
                    )
            elif poll_count <= 5 and not log_parsing_available:
                # Retry on early polls — logs may not be available until RUNNING
                run_steps = run_details.get("run_steps", [])
                if run_steps:
                    latest_step = max(run_steps, key=lambda s: s.get("index", 0))
                    test_logs = self._fetch_step_debug_logs(latest_step["id"], context)
                    if test_logs:
                        log_parsing_available = True
                        context.log.info(
                            f"Debug logs now available ({len(test_logs)} chars). "
                            f"Per-model monitoring active."
                        )
            else:
                context.log.info(f"Run {self.run_id} status: {status_name}")

            # 2. Parse debug logs for per-model results (works mid-step)
            log_failures = self._parse_debug_logs_safe(run_details, context)

            # 3. Check run_results.json artifacts (works per-step)
            artifact_failures = self._check_for_artifact_results(context)

            all_new_failures = log_failures + artifact_failures

            # 4. Fail fast?
            if self.fail_fast and all_new_failures:
                failed_models = ", ".join(f.unique_id for f in all_new_failures)
                context.log.error(
                    f"Fail-fast triggered. Cancelling dbt Cloud run {self.run_id}. "
                    f"Failed: {failed_models}"
                )
                self.cancel_run(context)
                raise Exception(
                    f"dbt Cloud run {self.run_id} cancelled — "
                    f"model failures detected: {failed_models}"
                )

            # 5. Terminal state?
            if run.status in TERMINAL_STATUSES:
                # Final artifact check
                self._check_for_artifact_results(context)

                total = len(self._all_results)
                failures = [r for r in self._all_results if r.status in FAILURE_STATUSES]

                if run.status == DbtCloudJobRunStatusType.ERROR:
                    context.log.error(
                        f"Run {self.run_id} finished with ERROR. "
                        f"{len(failures)} model failure(s) detected across "
                        f"{total} total results."
                    )
                    raise Exception(
                        f"dbt Cloud run {self.run_id} finished with ERROR. "
                        f"Failed models: "
                        f"{', '.join(f.unique_id for f in failures) or 'see dbt Cloud logs'}"
                    )
                if run.status == DbtCloudJobRunStatusType.CANCELLED:
                    raise Exception(
                        f"dbt Cloud run {self.run_id} was CANCELLED"
                    )

                context.log.info(
                    f"Run {self.run_id} completed successfully. "
                    f"{total} results captured during monitoring."
                )
                return

            time.sleep(self.poll_interval)

    # -- Run details fetching -----------------------------------------------

    def _get_run_details_safe(
        self, context: AssetExecutionContext
    ) -> dict[str, Any]:
        """Fetch run details with run_steps included.

        The dbt Cloud API returns step metadata (id, index, status) on the
        run endpoint when include_related=["run_steps"]. Debug logs are
        fetched separately per-step via _fetch_step_debug_logs.
        """
        try:
            response = self.client._make_request(
                method="get",
                endpoint=f"runs/{self.run_id}",
                base_url=self.client.api_v2_url,
                params={"include_related": '["run_steps"]'},
            )
            return response["data"]
        except Exception:
            # Fall back to plain run details without run_steps
            try:
                return self.client.get_run_details(self.run_id)
            except Exception as e:
                context.log.warning(
                    f"Failed to fetch run details for {self.run_id}: {e}"
                )
                raise

    def _fetch_step_debug_logs(
        self, step_id: int, context: AssetExecutionContext
    ) -> str:
        """Fetch debug logs for a specific run step.

        dbt Cloud API: GET /steps/{step_id}/?include_related=["debug_logs"]
        Returns the step's debug_logs field (truncated to last ~1000 lines).
        """
        try:
            response = self.client._make_request(
                method="get",
                endpoint=f"steps/{step_id}",
                base_url=self.client.api_v2_url,
                params={"include_related": '["debug_logs"]'},
            )
            step_data = response.get("data", {})
            return step_data.get("debug_logs", "") or step_data.get("logs", "") or ""
        except Exception as e:
            context.log.debug(f"Failed to fetch debug logs for step {step_id}: {e}")
            return ""

    # -- Debug log parsing (per-model, real-time) ---------------------------

    def _parse_debug_logs_safe(
        self,
        run_details: dict[str, Any],
        context: AssetExecutionContext,
    ) -> list[ModelResult]:
        """Parse debug logs for per-model results. Never raises."""
        try:
            return self._parse_debug_logs(run_details, context)
        except Exception as e:
            context.log.debug(f"Debug log parsing skipped: {e}")
            return []

    def _parse_debug_logs(
        self,
        run_details: dict[str, Any],
        context: AssetExecutionContext,
    ) -> list[ModelResult]:
        """Parse dbt debug logs for per-model success/failure lines.

        Fetches debug logs from the latest/active run step via the steps
        endpoint. dbt Cloud debug logs are a sliding window (~last 1000 lines),
        NOT an append-only log. We parse the full window on every call and use
        _seen_log_nodes for deduplication.
        """
        new_failures: list[ModelResult] = []

        log_text = self._get_logs_from_steps(run_details, context)
        if not log_text:
            return new_failures

        # Parse model result lines (OK / ERROR)
        for match in _MODEL_RESULT_PATTERN.finditer(log_text):
            _seq, _total, status_str, model_name = match.groups()
            if model_name in self._seen_log_nodes:
                continue
            self._seen_log_nodes.add(model_name)

            is_error = status_str.upper() == "ERROR"
            result = ModelResult(
                unique_id=model_name,
                status="error" if is_error else "ok",
                execution_time=0.0,
                message=match.group(0).strip(),
                source="logs",
            )
            self._all_results.append(result)

            if is_error:
                context.log.error(f"MODEL FAILED: {model_name}")
                new_failures.append(result)
            else:
                context.log.info(f"Model OK: {model_name}")

        # Parse test result lines (PASS / FAIL / WARN / SKIP)
        for match in _TEST_RESULT_PATTERN.finditer(log_text):
            _seq, _total, status_str, test_name = match.groups()
            if test_name in self._seen_log_nodes:
                continue
            self._seen_log_nodes.add(test_name)

            is_failure = status_str.upper() == "FAIL"
            result = ModelResult(
                unique_id=test_name,
                status=status_str.lower(),
                execution_time=0.0,
                message=match.group(0).strip(),
                source="logs",
            )
            self._all_results.append(result)

            if is_failure:
                context.log.error(f"TEST FAILED: {test_name}")
                new_failures.append(result)
            elif status_str.upper() == "WARN":
                context.log.warning(f"Test warning: {test_name}")
            else:
                context.log.info(f"Test {status_str.lower()}: {test_name}")

        # Catch explicit error patterns that may not match the N of M format
        for pattern in (_RUNTIME_ERROR_PATTERN, _COMPILATION_ERROR_PATTERN, _DATABASE_ERROR_PATTERN):
            for match in pattern.finditer(log_text):
                node_name = match.group(1)
                if node_name in self._seen_log_nodes:
                    continue
                self._seen_log_nodes.add(node_name)

                result = ModelResult(
                    unique_id=node_name,
                    status="error",
                    execution_time=0.0,
                    message=match.group(0).strip(),
                    source="logs",
                )
                self._all_results.append(result)
                context.log.error(f"ERROR: {match.group(0).strip()}")
                new_failures.append(result)

        return new_failures

    def _get_logs_from_steps(
        self, run_details: dict[str, Any], context: AssetExecutionContext
    ) -> str:
        """Fetch debug logs from each run step via the steps API.

        The dbt Cloud API structure:
        1. GET /runs/{id}/?include_related=["run_steps"] → list of steps
        2. GET /steps/{step_id}/?include_related=["debug_logs"] → step logs

        Debug logs live on the step endpoint, not the run endpoint.
        We fetch logs from all steps and concatenate them.
        """
        run_steps = run_details.get("run_steps", [])
        if not run_steps:
            return ""

        parts: list[str] = []
        # Sort by index to process steps in order
        for step in sorted(run_steps, key=lambda s: s.get("index", 0)):
            step_id = step.get("id")
            if not step_id:
                continue
            log_text = self._fetch_step_debug_logs(step_id, context)
            if log_text:
                parts.append(log_text)

        return "\n".join(parts)

    # -- Artifact checking (per-step) --------------------------------------

    def _check_for_artifact_results(
        self, context: AssetExecutionContext
    ) -> list[ModelResult]:
        """Fetch run_results.json and log any new model results.

        For multi-step jobs, this updates as each step completes, giving
        structured results with full unique_ids and execution times.

        Returns only NEW failures found in this check.
        """
        new_failures: list[ModelResult] = []

        try:
            artifacts = self.client.list_run_artifacts(self.run_id)
            if "run_results.json" not in artifacts:
                return new_failures

            results_json = self.client.get_run_results_json(self.run_id)
            for result in results_json.get("results", []):
                unique_id = result.get("unique_id", "")
                if unique_id in self._seen_artifact_nodes:
                    continue

                self._seen_artifact_nodes.add(unique_id)
                status = result.get("status", "")
                model_result = ModelResult(
                    unique_id=unique_id,
                    status=status,
                    execution_time=result.get("execution_time", 0.0),
                    message=result.get("message", ""),
                    source="artifacts",
                )
                self._all_results.append(model_result)

                if status in FAILURE_STATUSES:
                    context.log.error(
                        f"FAILED (run_results.json): {unique_id} — "
                        f"{model_result.message}"
                    )
                    new_failures.append(model_result)
                else:
                    context.log.info(
                        f"OK (run_results.json): {unique_id} "
                        f"({model_result.execution_time:.1f}s)"
                    )
        except Exception:
            pass

        return new_failures

    # -- Run cancellation ---------------------------------------------------

    def cancel_run(self, context: AssetExecutionContext) -> None:
        """Cancel the dbt Cloud run via the Admin API."""
        try:
            self.client._make_request(
                method="post",
                endpoint=f"runs/{self.run_id}/cancel/",
                base_url=self.client.api_v2_url,
            )
            context.log.warning(f"Cancelled dbt Cloud run {self.run_id}")
        except Exception as e:
            context.log.error(f"Failed to cancel run {self.run_id}: {e}")

    # -- Final event generation ---------------------------------------------

    def get_asset_events(
        self,
        manifest: Mapping[str, Any],
        dagster_dbt_translator: DagsterDbtTranslator | None = None,
        context: AssetExecutionContext | None = None,
    ) -> Iterator[
        Union[AssetCheckEvaluation, AssetCheckResult, AssetMaterialization, Output]
    ]:
        """Fetch final run_results.json and yield proper Dagster asset events.

        Call this after poll() completes successfully to emit Output /
        AssetMaterialization / AssetCheckResult events into the Dagster run.
        The final run_results.json is the source of truth — the mid-run log
        parsing is for alerting only.
        """
        try:
            run_results_json = self.client.get_run_results_json(self.run_id)
        except Exception:
            return

        run_results = DbtCloudJobRunResults.from_run_results_json(run_results_json)
        yield from run_results.to_default_asset_events(
            client=self.client,
            manifest=manifest,
            dagster_dbt_translator=dagster_dbt_translator,
            context=context,
        )
