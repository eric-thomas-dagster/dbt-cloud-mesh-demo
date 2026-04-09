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

# Actual dbt Cloud debug log format (verified against real output):
#
# Models:
#   21:34:34  1 of 21 OK created sql table model JAFFLE_SHOP.customer_metrics ... [\x1b[32mSUCCESS 100\x1b[0m in 0.90s]
#   21:35:05  18 of 21 ERROR creating sql table model JAFFLE_SHOP.orders_partitioned ... [\x1b[31mERROR\x1b[0m in 0.01s]
#
# Tests:
#   21:34:38  6 of 21 PASS not_null_customer_metrics_customer_id ... [\x1b[32mPASS\x1b[0m in 0.21s]
#   21:34:39  9 of 21 FAIL 3 not_null_model_col ... [\x1b[31mFAIL 3\x1b[0m in 0.8s]
#
# Summary:
#   21:35:07  Done. PASS=20 WARN=0 ERROR=1 SKIP=0 NO-OP=0 TOTAL=21

# Strip ANSI color codes before parsing
_ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")

# Model results: "N of M OK created ... SCHEMA.model_name" or "N of M ERROR creating ... SCHEMA.model_name"
# The model name is SCHEMA.name (contains a dot) and appears after the materialization keywords.
_MODEL_RESULT_PATTERN = re.compile(
    r"(\d+)\s+of\s+(\d+)\s+"                       # "N of M "
    r"(OK|ERROR)\s+"                                # status
    r"(?:created|creating)\s+"                      # verb
    r"(?:sql\s+)?(?:table|view|incremental)\s+"     # materialization type (may have "sql " prefix)
    r"(?:model|snapshot|seed)\s+"                    # resource type
    r"(\S+\.\S+)",                                  # SCHEMA.model_name
)

# Test results: "N of M PASS test_name" or "N of M FAIL 3 test_name"
# Test names do NOT contain dots — they're just the test name.
_TEST_RESULT_PATTERN = re.compile(
    r"(\d+)\s+of\s+(\d+)\s+"                       # "N of M "
    r"(PASS|FAIL|WARN|SKIP)\s+"                     # status
    r"(?:\d+\s+)?"                                  # optional failure count
    r"(\S+)",                                       # test name (no dot)
)

# Explicit error patterns that may appear outside the N of M format
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

            # Log status on every poll so the user can see we're alive
            context.log.info(f"Run {self.run_id} status: {status_name}")

            # On first poll, log run URL and check monitoring capabilities
            if poll_count == 1:
                run_url = run.url or run_details.get("href", "")
                if run_url:
                    context.log.info(f"dbt Cloud run URL: {run_url}")

            # Try to activate debug log parsing
            if not log_parsing_available:
                run_steps = run_details.get("run_steps", [])
                if run_steps:
                    latest_step = max(run_steps, key=lambda s: s.get("index", 0))
                    test_logs = self._fetch_step_debug_logs(latest_step["id"], context)
                    if test_logs:
                        log_parsing_available = True
                        context.log.info("Per-model log monitoring active.")

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
                # Return normally — let get_asset_events() try to yield
                # partial results for models that completed before cancellation.
                # If run_results.json isn't available (too early), no events
                # are yielded and all assets fail, which is acceptable for
                # fail_fast — you stopped early on purpose.
                return

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
                    # Don't raise here — return and let get_asset_events()
                    # yield Output events for the models that succeeded.
                    # Models that failed won't get an Output, so Dagster
                    # will mark only those assets as failed.
                    return

                if run.status == DbtCloudJobRunStatusType.CANCELLED:
                    context.log.warning(
                        f"Run {self.run_id} was CANCELLED. "
                        f"Attempting to yield partial results if available."
                    )
                    # Return normally — get_asset_events() will try to fetch
                    # run_results.json. If available, partial results are yielded.
                    # If not, no events → all assets show as failed.
                    return

                context.log.info(
                    f"Run {self.run_id} completed successfully. "
                    f"{total} results captured during monitoring."
                )
                return

            time.sleep(self.poll_interval)

    # -- Run details fetching -----------------------------------------------

    def _dbt_cloud_get(self, endpoint: str, params: dict | None = None) -> dict:
        """Make a GET request to the dbt Cloud API.

        Uses requests directly instead of client._make_request to avoid
        version-specific return type differences in dagster-dbt.
        """
        import requests as req

        url = f"{self.client.api_v2_url}/{endpoint}"
        resp = req.get(
            url,
            headers={
                "Authorization": f"Token {self.client.token}",
                "Content-Type": "application/json",
            },
            params=params,
            timeout=self.client.request_timeout,
        )
        resp.raise_for_status()
        return resp.json()

    def _get_run_details_safe(
        self, context: AssetExecutionContext
    ) -> dict[str, Any]:
        """Fetch run details with run_steps included.

        The dbt Cloud API returns step metadata (id, index, status) on the
        run endpoint when include_related=["run_steps"]. Debug logs are
        fetched separately per-step via _fetch_step_debug_logs.
        """
        try:
            data = self._dbt_cloud_get(
                f"runs/{self.run_id}",
                params={"include_related": '["run_steps"]'},
            )
            return data["data"]
        except Exception as e:
            context.log.warning(
                f"include_related request failed: {e}. "
                f"Falling back to plain run details (no run_steps)."
            )
            try:
                return self.client.get_run_details(self.run_id)
            except Exception as e2:
                context.log.warning(
                    f"Failed to fetch run details for {self.run_id}: {e2}"
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
            data = self._dbt_cloud_get(
                f"steps/{step_id}",
                params={"include_related": '["debug_logs"]'},
            )
            step_data = data.get("data", {})
            debug_logs = step_data.get("debug_logs", "") or ""
            logs = step_data.get("logs", "") or ""
            return debug_logs or logs
        except Exception:
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

        Fetches debug logs from run steps via the steps API. Strips ANSI
        color codes before parsing. Uses _seen_log_nodes for deduplication
        across polls (debug logs are a sliding window, not append-only).
        """
        new_failures: list[ModelResult] = []

        log_text = self._get_logs_from_steps(run_details, context)
        if not log_text:
            return new_failures

        # Strip ANSI color codes (dbt Cloud logs contain \x1b[32m etc.)
        log_text = _ANSI_ESCAPE.sub("", log_text)

        # Parse model result lines (OK / ERROR)
        # Must run BEFORE test pattern since test pattern is more general
        model_names_this_parse: set[str] = set()
        for match in _MODEL_RESULT_PATTERN.finditer(log_text):
            _seq, _total, status_str, model_name = match.groups()
            model_names_this_parse.add(model_name)
            if model_name in self._seen_log_nodes:
                continue
            self._seen_log_nodes.add(model_name)

            is_error = status_str.upper() == "ERROR"
            result = ModelResult(
                unique_id=model_name,
                status="error" if is_error else "success",
                execution_time=0.0,
                message=match.group(0).strip(),
                source="logs",
            )
            self._all_results.append(result)

            if is_error:
                context.log.error(f"MODEL FAILED (logs): {model_name}")
                new_failures.append(result)
            else:
                context.log.info(f"Model OK (logs): {model_name}")

        # Parse test result lines (PASS / FAIL / WARN / SKIP)
        # Skip any match that was already captured by the model pattern
        for match in _TEST_RESULT_PATTERN.finditer(log_text):
            _seq, _total, status_str, test_name = match.groups()
            # Skip if this looks like a model name (contains dot) — already handled above
            if "." in test_name:
                continue
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
                context.log.error(f"TEST FAILED (logs): {test_name}")
                new_failures.append(result)
            elif status_str.upper() == "WARN":
                context.log.warning(f"Test warning (logs): {test_name}")
            else:
                context.log.info(f"Test {status_str.lower()} (logs): {test_name}")

        # Catch explicit error patterns outside the N of M format
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
                context.log.error(f"ERROR (logs): {match.group(0).strip()}")
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

            # Extract invocation_id from metadata
            invocation_id = results_json.get("metadata", {}).get("invocation_id", "")

            for result in results_json.get("results", []):
                unique_id = result.get("unique_id", "")
                if unique_id in self._seen_artifact_nodes:
                    continue

                self._seen_artifact_nodes.add(unique_id)
                status = result.get("status", "")
                exec_time = result.get("execution_time", 0.0)
                message = result.get("message", "")
                adapter_response = result.get("adapter_response", {})

                model_result = ModelResult(
                    unique_id=unique_id,
                    status=status,
                    execution_time=exec_time,
                    message=message,
                    source="artifacts",
                )
                self._all_results.append(model_result)

                # Build timing info from result
                timing = result.get("timing", [])
                completed_at = ""
                if timing:
                    completed_at = timing[-1].get("completed_at", "")

                # Build rich log line matching OOTB metadata quality
                metadata_parts = [
                    f"unique_id={unique_id}",
                    f"status={status}",
                    f"execution_time={exec_time:.2f}s",
                ]
                if invocation_id:
                    metadata_parts.append(f"invocation_id={invocation_id}")
                if completed_at:
                    metadata_parts.append(f"completed_at={completed_at}")
                if adapter_response:
                    rows = adapter_response.get("rows_affected", adapter_response.get("_message", ""))
                    if rows:
                        metadata_parts.append(f"rows_affected={rows}")

                metadata_str = " | ".join(metadata_parts)

                if status in FAILURE_STATUSES:
                    context.log.error(
                        f"FAILED: {unique_id} — {message} [{metadata_str}]"
                    )
                    new_failures.append(model_result)
                else:
                    context.log.info(
                        f"OK: {unique_id} ({exec_time:.1f}s) [{metadata_str}]"
                    )
        except Exception:
            pass

        return new_failures

    # -- Run cancellation ---------------------------------------------------

    def cancel_run(self, context: AssetExecutionContext) -> None:
        """Cancel the dbt Cloud run via the Admin API."""
        import requests as req

        try:
            url = f"{self.client.api_v2_url}/runs/{self.run_id}/cancel/"
            resp = req.post(
                url,
                headers={
                    "Authorization": f"Token {self.client.token}",
                    "Content-Type": "application/json",
                },
                timeout=self.client.request_timeout,
            )
            resp.raise_for_status()
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
