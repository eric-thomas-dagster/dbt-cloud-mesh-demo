"""Example: Triggered dbt Cloud assets with mid-run per-model monitoring.

This demonstrates an alternative to the observe-only or standard orchestrate
patterns. Instead of waiting for the full dbt Cloud run to complete before
reporting results, Dagster actively monitors for per-model failures DURING
execution by parsing dbt Cloud's debug logs in real time.

How it works:
  1. Dagster triggers the dbt Cloud job
  2. Every poll_interval seconds, fetches the run's debug logs
  3. Parses dbt's output for per-model OK/ERROR lines as they happen
  4. On first failure: cancels the dbt Cloud run and fails the Dagster run
  5. No more engineers sitting and watching — Dagster handles it

To use this in your project:
  1. Set the workspace credentials below (or import from your component config)
  2. Add the assets/sensors to your Definitions
  3. Configure Dagster+ alerting (Slack/PagerDuty/email) for run failures

Note: Debug log parsing is the best available approach for per-model
granularity mid-run. dbt Cloud's Discovery API and run_results.json only
update after a run/step completes. For true real-time per-model streaming,
consider dbt Core via Dagster's DbtCliResource.
"""

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DagsterDbtTranslatorSettings, dbt_cloud_assets
from dagster_dbt.cloud_v2.resources import DbtCloudCredentials, DbtCloudWorkspace
from dagster_dbt.cloud_v2.types import DbtCloudRun

from dbt_cloud_mesh_demo.defs.run_monitor import DbtCloudRunMonitor


# -- Configuration ----------------------------------------------------------
# Adapt these to your dbt Cloud workspace.

workspace = DbtCloudWorkspace(
    credentials=DbtCloudCredentials(
        account_id=dg.EnvVar.int("DBT_CLOUD_ACCOUNT_ID"),
        token=dg.EnvVar("DBT_CLOUD_TOKEN"),
        access_url="https://cloud.getdbt.com",
    ),
    project_id=dg.EnvVar.int("DBT_CLOUD_SILVER_PROJECT_ID"),
    environment_id=dg.EnvVar.int("DBT_CLOUD_SILVER_ENVIRONMENT_ID"),
)

translator = DagsterDbtTranslator(
    settings=DagsterDbtTranslatorSettings(
        enable_source_metadata=True,
        enable_dbt_selection_by_name=True,
    ),
)


# -- Asset definition -------------------------------------------------------

@dbt_cloud_assets(
    workspace=workspace,
    name="silver_triggered_monitored",
    dagster_dbt_translator=translator,
)
def silver_triggered_assets(
    context: dg.AssetExecutionContext,
    dbt_cloud: DbtCloudWorkspace,
):
    """Trigger a dbt Cloud run with per-model failure monitoring.

    Instead of the default wait-for-completion pattern:
        result = dbt_cloud.cli(["build"], context=context)
        yield from result.wait()

    We trigger the run ourselves and use DbtCloudRunMonitor to parse dbt's
    debug logs on each poll for per-model OK/ERROR results in real time.
    """
    client = dbt_cloud.get_client()
    workspace_data = dbt_cloud.get_or_fetch_workspace_data()

    # Trigger the run
    run_details = client.trigger_job_run(
        job_id=workspace_data.adhoc_job_id,
        steps_override=["dbt build"],
    )
    run = DbtCloudRun.from_run_details(run_details)
    context.log.info(f"Triggered dbt Cloud run {run.id}")
    if run.url:
        context.log.info(f"dbt Cloud UI: {run.url}")

    # Monitor with per-model failure detection via debug log parsing.
    #
    # poll_interval: How often to fetch and parse debug logs (seconds).
    #   This runs inside asset execution (not a sensor), so there is no
    #   minimum interval. 5s keeps up with the ~1000-line log window.
    #   For API rate concerns, increase to 10-15s.
    #
    # fail_fast: On the first model failure detected in the logs,
    #   cancel the dbt Cloud run and fail this Dagster run immediately.
    #   Set to False to let the full run complete regardless of errors.
    monitor = DbtCloudRunMonitor(
        client=client,
        run_id=run.id,
        poll_interval=5,
        fail_fast=True,
    )
    monitor.poll(context, timeout=7200)  # 2 hour timeout

    # Yield standard Dagster events (Output, AssetCheckResult, etc.)
    # from the final run_results.json — this is the source of truth.
    yield from monitor.get_asset_events(
        manifest=workspace_data.manifest,
        dagster_dbt_translator=translator,
        context=context,
    )
