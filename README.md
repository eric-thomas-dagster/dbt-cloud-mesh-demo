# dbt Cloud Mesh Demo

Dagster project demonstrating orchestration of two **dbt Cloud** projects that use dbt mesh for cross-project references. One project handles bronze-to-silver transformations, the other handles silver-to-gold and references silver models via dbt mesh.

## Observe vs. Orchestrate Mode

The `DbtCloudMeshComponent` supports two modes, controlled by the `mode` attribute in `defs.yaml`:

| | **Observe** | **Orchestrate** |
|---|---|---|
| **Who triggers dbt Cloud runs?** | External (dbt Cloud scheduler, CI, manual) | Dagster (materialize button, Declarative Automation) |
| **Asset type** | `AssetSpec` (non-materializable) | `AssetsDefinition` (materializable) |
| **Polling sensor** | Auto-created (watches for run completions) | Optional (`create_sensor: true`) |
| **What you get in Dagster** | Full asset graph, lineage, run history | Full asset graph, lineage, run history + ability to trigger runs |
| **Use when** | dbt Cloud owns scheduling; Dagster provides visibility | Dagster owns the full pipeline |

### Observe mode (recommended starting point)

```yaml
# defs.yaml
attributes:
  mode: observe
  # create_sensor is optional — observe mode auto-creates one if missing
```

Assets appear in the Dagster asset graph with full lineage. When dbt Cloud runs complete externally, the polling sensor detects them and records materializations in Dagster. You get run history, metadata, and cross-project lineage without Dagster triggering anything.

### Orchestrate mode

```yaml
# defs.yaml
attributes:
  mode: orchestrate   # this is the default
  create_sensor: true  # optional — for also observing externally-triggered runs
```

Assets are materializable from the Dagster UI or via Declarative Automation. Dagster triggers dbt Cloud runs directly.

### Orchestrate mode with run monitoring

```yaml
# defs.yaml — monitor and log failures in real time, but let the run finish
attributes:
  mode: orchestrate
  monitor_runs: true    # enable per-model mid-run monitoring
  poll_interval: 5      # check debug logs every 5 seconds
```

```yaml
# defs.yaml — cancel the run on first failure
attributes:
  mode: orchestrate
  monitor_runs: true
  fail_fast: true       # cancel dbt Cloud run on first model failure
  poll_interval: 5
```

When `monitor_runs: true`, Dagster fetches debug logs from dbt Cloud's steps API every `poll_interval` seconds during execution. Individual model results (OK/ERROR) are logged to the Dagster run log as they happen — not after the entire job finishes. Pair with Dagster+ alerting (Slack/PagerDuty/email) to eliminate manual monitoring of long-running jobs.

Two monitoring modes:

| | `fail_fast: false` (default) | `fail_fast: true` |
|---|---|---|
| **On model failure** | Log it immediately, keep the run going | Cancel the dbt Cloud run, fail the Dagster run |
| **Partial results** | All successful models materialized, failed ones marked failed | Models completed before cancellation materialized |
| **Step status** | `STEP_FAILURE` (matches OOTB) | `STEP_FAILURE` with cancel confirmation |
| **Use when** | You want to see ALL failures in one run | Any failure means the output is bad, or you want to save compute |

### How it works under the hood

1. `workspace.cli(["build"])` triggers the dbt Cloud run (handles subset selection for re-execution)
2. Every `poll_interval` seconds:
   - `GET /runs/{id}/?include_related=["run_steps"]` for run status and step metadata
   - `GET /steps/{step_id}/?include_related=["debug_logs"]` for per-model results from dbt's log output
   - `GET /runs/{id}/artifacts/run_results.json` for structured results when steps complete
3. Debug logs are stripped of ANSI color codes and parsed with regex for `N of M OK/ERROR` model lines and `PASS/FAIL` test lines
4. On completion, `DbtCloudJobRunResults.to_default_asset_events()` yields proper Dagster materializations and check evaluations (same as OOTB)
5. If failures occurred, `dagster.Failure` is raised after yielding partial results (matching OOTB behavior)

## What is dbt Mesh?

dbt Mesh is a multi-project architecture pattern in dbt Cloud (Enterprise and Enterprise+ plans) that allows separate dbt projects to reference each other's models. Instead of one monolithic dbt project, teams split their transformations into independent projects (e.g. silver and gold) that share data products via cross-project `{{ ref() }}` calls.

Key features:
- **Cross-project references**: `{{ ref('silver_project', 'customers') }}` in the gold project resolves to the silver project's `customers` model
- **Public models**: Only models marked `access: public` can be referenced across projects
- **Model contracts**: Enforce data shape expectations at project boundaries
- **Independent deployment**: Each project has its own dbt Cloud jobs, environments, and deployment schedules

When two projects use mesh, the downstream project's manifest contains **both** its own models and references to the upstream project's public models. A dbt Cloud run of the downstream project may execute or reference upstream models depending on the configuration.

For more details, see [dbt Mesh documentation](https://docs.getdbt.com/best-practices/how-we-mesh/mesh-1-intro).

## How dbt Mesh Works with Dagster

In Dagster, each dbt Cloud project becomes a component that produces assets. With dbt mesh, the downstream project's manifest includes upstream models, which creates two challenges:

1. **Duplicate assets**: Both the silver and gold components would try to create assets for silver models
2. **Sensor cross-contamination**: The gold sensor would emit materialization events for silver models included in gold run results

The solution: `exclude: "package:silver_project"` on the gold component prevents silver models from becoming gold assets, and the `DbtCloudMeshComponent`'s mesh-aware sensor filters out cross-project materialization events. Dependency edges from gold models to silver asset keys are preserved — Dagster resolves them to the silver component's actual assets.

## Why the OOTB DbtCloudComponent Doesn't Work for dbt Mesh

The standard `DbtCloudComponent` has a **sensor problem** that affects all dbt mesh deployments — single code location or multi-code-location.

When two dbt Cloud projects use dbt mesh, the downstream project's (gold) run results include models from the upstream project (silver) — because dbt mesh pulls them into execution via packages. The OOTB polling sensor processes **all** run results and emits `AssetMaterialization` events for every successful model, including the upstream models.

This causes **double materialization of upstream assets**:

- Silver sensor materializes silver assets from silver runs ✓
- Gold sensor **also** materializes silver assets from gold runs ✗

The result:
- Silver asset freshness timestamps get overwritten by gold run timing
- Silver assets appear "materialized" even if only the gold run completed
- Automation conditions based on freshness fire incorrectly
- Run history shows silver assets materialized by both sensors

In **multi-code-location** deployments, it's worse — the gold sensor crashes entirely because `sorted_asset_events` tries to look up silver asset keys in the gold code location's toposorted list, and they don't exist:

```
AssetKey(['evaluator', 'base_exposure_relationships']) is not in list
```

**Use `DbtCloudMeshComponent` for any dbt mesh deployment.** It includes a mesh-aware sensor that filters out materialization events for external package models, preventing both the double-materialization problem (single code location) and the sensor crash (multi-code-location).

## Single Code Location

```yaml
# silver_cloud/defs.yaml
type: dbt_cloud_mesh_demo.components.dbt_cloud_mesh_component.DbtCloudMeshComponent
attributes:
  workspace:
    account_id: 12345
    token: "{{ env.DBT_CLOUD_TOKEN }}"
    project_id: 100001
    environment_id: 200001
  create_sensor: true
```

```yaml
# gold_cloud/defs.yaml
type: dbt_cloud_mesh_demo.components.dbt_cloud_mesh_component.DbtCloudMeshComponent
attributes:
  workspace:
    account_id: 12345
    token: "{{ env.DBT_CLOUD_TOKEN }}"
    project_id: 100002
    environment_id: 200002
  exclude: "package:silver_project"
  create_sensor: true
```

This works because:

- **No duplicate assets.** The silver component creates assets for silver models. The gold component's `exclude` prevents creating them again.
- **Lineage is preserved.** `exclude` only prevents silver models from becoming assets in the gold component — it does **not** strip the dependency edges. Gold models still have `deps` pointing to silver asset keys.
- **The mesh-aware sensor filters correctly.** When the gold run completes with silver models in the results, the sensor only emits materializations for gold assets. Silver materializations are dropped — the silver sensor handles those independently.

## Multi-Code-Location Deployments

If your silver and gold projects are in **separate code locations**, lineage still works - Dagster builds a unified asset graph across all code locations in a workspace. Gold's dependency on `AssetKey(["customers"])` resolves to the silver code location's asset automatically.

However, the **polling sensor will crash** with an error like:

```
AssetKey(['evaluator', 'base_exposure_relationships']) is not in list
```

This happens because the sensor's internal `sorted_asset_events` function sorts materialization events using `repository_def.asset_graph.toposorted_asset_keys`, which is scoped to the **current code location's** asset graph. When the gold dbt Cloud run completes, the run results include silver models. The sensor tries to look up those silver asset keys in the gold code location's toposorted list, and they're not there.

### Why This Only Affects dbt Cloud (Not dbt Core)

With `DbtProjectComponent` (dbt Core), Dagster runs dbt directly via CLI and controls exactly which models execute. The `exclude` filter means silver models are never run, so there are no materialization events to emit for them. With dbt Cloud, runs happen externally and the sensor observes **all** completed run results, including silver models that were part of the run.

### The Fix: DbtCloudMeshComponent

The `DbtCloudMeshComponent` solves the sensor problem by replacing the standard polling sensor with a **mesh-aware sensor** that filters out materialization events for models belonging to external packages. Use it for all dbt mesh deployments regardless of code location setup.

```yaml
# gold_cloud/defs.yaml
type: dbt_cloud_mesh_demo.components.dbt_cloud_mesh_component.DbtCloudMeshComponent
attributes:
  workspace:
    account_id: 12345
    token: "{{ env.DBT_CLOUD_TOKEN }}"
    project_id: 100002
    environment_id: 200002
  exclude: "package:silver_project"
  create_sensor: true
  external_packages:
    silver_project:
      key_prefix: ["silver_project"]
      group_name: "silver"
```

The `external_packages` config tells the mesh-aware sensor which packages to filter. The silver component's own sensor handles silver model materializations independently.

### Cross-layer lineage (group_overrides)

For lineage to wire up across layers (bronze → silver → gold), the asset keys produced by each component must match. By default, the dbt translator generates asset keys from the dbt model name — but upstream assets from other systems (e.g. Databricks bronze tables) or other dbt projects may use different key structures.

`group_overrides` assigns Dagster groups from YAML based on dbt resource types or fqn path segments. This helps organize assets into the correct groups for visual clarity in the asset graph, and ensures that models in different layers are visually grouped correctly:

```yaml
  group_overrides:
    seed: raw
    staging: bronze
    silver: silver
    gold: gold
```

Matches by resource type first (e.g. `seed`), then by each fqn path segment (e.g. `staging`, `silver`). Falls back to the dbt-defined group if no override matches.

For more complex key mapping — like mapping dbt sources to existing Dagster assets from Databricks or another system — override `get_asset_key` on a custom translator. For example, a silver project's translator can map `source.bronze.*` to `AssetKey(["bronze", table_name])` to match upstream Databricks assets, and a gold project's translator can prefix silver package models to match the silver component's keys. This ensures the full bronze → silver → gold lineage connects in the asset graph.

## Setup for Your dbt Cloud Account

### 1. Get your dbt Cloud credentials

You need:
- **Account ID**: Found in your dbt Cloud URL (`https://cloud.getdbt.com/deploy/{ACCOUNT_ID}/...`)
- **API Token**: Generate a service token at Account Settings > Service Tokens (requires at least "Job Admin" and "Metadata Only" permissions)
- **Project IDs**: One for each dbt project (silver and gold). Found in Project Settings.
- **Environment IDs**: The deployment environment for each project. Found in Environment Settings.

### 2. Set your token

```bash
export DBT_CLOUD_TOKEN="your-dbt-cloud-service-token"
```

### 3. Update the defs.yaml files

Edit `src/dbt_cloud_mesh_demo/defs/silver_cloud/defs.yaml` and `gold_cloud/defs.yaml` with your account, project, and environment IDs.

### 4. Enable the polling sensor

Once connected to real dbt Cloud, set `create_sensor: true` in both `defs.yaml` files.

### 5. Refresh the dbt Cloud state

Remove the pre-baked mock state and refresh from dbt Cloud:

```bash
rm -rf src/dbt_cloud_mesh_demo/defs/.local_defs_state/DbtCloudComponent__*
```

Set `refresh_if_dev: true` in the `defs_state` block of each `defs.yaml` (or remove the `defs_state` block entirely to use the default):

```yaml
  defs_state:
    management_type: LOCAL_FILESYSTEM
    refresh_if_dev: true
```

Then refresh:

```bash
uv run dg plus deploy refresh-defs-state
```

### 6. Run Dagster

```bash
uv run dg dev
```

Open http://localhost:3000 to see the asset graph.

## Auto-Refreshing Definitions on dbt Cloud Changes (Experimental)

When models change in dbt Cloud (new models added, schemas modified, configs updated), Dagster's cached definitions state becomes stale -- the asset graph won't reflect the changes until you either redeploy or manually run:

```bash
dg plus deploy --organization <org> --deployment <deployment> refresh-defs-state
```

For local development, targeted refresh by key is available now:
```bash
dg utils refresh-defs-state --defs-state-key DbtCloudComponent[100002-200002]
```

This project includes a **`dbt_cloud_state_refresh_sensor`** that automates this. It polls dbt Cloud for new successful runs across all configured workspaces. When a new run is detected, it fetches the run's manifest and compares a **fingerprint** (node IDs + file checksums) against the cached fingerprint. Only when the project structure actually changed (new models, modified SQL, schema changes) does it trigger a refresh and reload the code location.

Routine runs that don't alter the project structure are ignored -- no unnecessary refreshes.

### Setup

Set these environment variables in your Dagster Cloud deployment (as secrets where appropriate):

```bash
# dbt Cloud API access (shared with workspace components)
DBT_CLOUD_ACCOUNT_ID="12345"
DBT_CLOUD_TOKEN="your-dbt-cloud-token"

# Dagster Cloud API token — required for state refresh and code location reload.
# NOT auto-provided. Generate at Settings > Tokens in your Dagster Cloud org.
DAGSTER_CLOUD_API_TOKEN="user:your-dagster-cloud-token"

# Per-workspace project/environment IDs
DBT_CLOUD_SILVER_PROJECT_ID="100001"
DBT_CLOUD_SILVER_ENVIRONMENT_ID="200001"
DBT_CLOUD_GOLD_PROJECT_ID="100002"
DBT_CLOUD_GOLD_ENVIRONMENT_ID="200002"
```

Dagster Cloud auto-provides `DAGSTER_CLOUD_DEPLOYMENT_NAME`, `DAGSTER_CLOUD_ORGANIZATION`, `DAGSTER_CLOUD_URL`, and `DAGSTER_CLOUD_LOCATION_NAME` -- no action needed for those.

Edit the `WORKSPACES` list in `defs/dbt_cloud_state_refresh_sensor.py` to match your workspace env var names.

### How it works

1. Sensor polls dbt Cloud every 5 minutes (configurable via `POLL_INTERVAL_SECONDS`)
2. For each workspace, fetches the latest successful run
3. If a new run is found, fetches its manifest and computes a fingerprint (node IDs + checksums)
4. Compares against the cached fingerprint -- if identical, no refresh needed (just a routine run)
5. If the fingerprint changed:
   - Triggers `dg plus deploy refresh-defs-state` to update cached state (attempts `--defs-state-key` for targeted refresh, falls back to full refresh if not yet supported)
   - Reloads the code location via the Dagster Cloud GraphQL `reloadRepositoryLocation` mutation so the running code server picks up the new definitions

The defs-state-key follows the pattern `DbtCloudComponent[{project_id}-{environment_id}]`, matching what `DbtCloudComponent.defs_state_config` generates.

The sensor ships with `default_status: STOPPED` -- enable it in the Dagster UI when ready.

## Running with Mock State (No dbt Cloud Required)

This project ships with pre-baked state files so you can explore the Dagster UI without dbt Cloud credentials:

```bash
export DBT_CLOUD_TOKEN=placeholder
uv run dg dev
```

The asset graph, lineage, and groups will render correctly. Materialization and the polling sensor will not function without real credentials.

## Changelog

### Mid-run per-model monitoring (monitor_runs)

Added `monitor_runs`, `fail_fast`, and `poll_interval` attributes to `DbtCloudMeshComponent`. In orchestrate mode with `monitor_runs: true`, the component replaces the standard wait-for-completion execution with active mid-run monitoring — no custom Python code needed.

```yaml
# Just add to defs.yaml
attributes:
  monitor_runs: true    # enable per-model monitoring
  fail_fast: false      # true = cancel on first failure, false = log and continue
  poll_interval: 5
```

Under the hood, the component fetches debug logs from dbt Cloud's steps API (`GET /steps/{id}/?include_related=["debug_logs"]`) every `poll_interval` seconds and parses them for per-model OK/ERROR results. Uses `requests` directly for API calls (avoids `dagster-dbt` internal API version differences).

- **Per-model granularity**: Detects model results as dbt logs them, not after the run/step completes
- **Two monitoring modes**: `fail_fast: true` cancels on first failure; `fail_fast: false` logs failures but lets the run continue so you see all errors
- **Partial result yielding**: Successful models are materialized even when the run has errors — matching OOTB behavior. `dagster.Failure` raised after yielding partials.
- **ANSI-safe log parsing**: Strips `\x1b[32m` etc. color codes from dbt Cloud debug logs before regex matching
- **Integrated into the component**: No separate Python asset code required — just YAML config
- **Re-execute from failure**: Subset materialization works via `workspace.cli()` context — same as OOTB
- **Cancel endpoint**: `POST /runs/{id}/cancel/` tested and working
- **`key_prefix` support**: Run multiple components against the same dbt Cloud project without asset key collisions
- **Namespaced sensors**: Sensor names incorporate `key_prefix` to avoid duplicate definition errors

Tested against real dbt Cloud (Snowflake adapter) with:
- Fast models, slow models (25s+), compilation errors, database errors
- `fail_fast: true` — cancel + partial results verified
- `fail_fast: false` — all failures captured, OOTB parity confirmed
- Side-by-side comparison with OOTB `DbtCloudComponent`

Use case: Engineers no longer need to manually monitor multi-hour dbt Cloud jobs. Dagster detects failures in seconds and alerts via Dagster+ notifications (Slack/PagerDuty/email).

Known limitation: Debug log parsing relies on dbt's console output format (verified against Snowflake adapter). The regex matches `N of M OK/ERROR created ... SCHEMA.model_name` for models and `N of M PASS/FAIL test_name` for tests. Debug logs are only available once a step is RUNNING (not QUEUED/STARTING) and are truncated to ~1000 lines. For true real-time per-model streaming without log parsing, use dbt Core via `DbtCliResource`.

### Observe vs. Orchestrate mode

Added a `mode` attribute to `DbtCloudMeshComponent` (`"observe"` or `"orchestrate"`, defaults to `"orchestrate"`).

**Observe mode** converts all materializable `AssetsDefinition` objects to non-materializable `AssetSpec` objects. dbt Cloud runs are triggered externally (via dbt Cloud scheduler, CI, or manual); Dagster provides full asset graph visibility, cross-project lineage, and run history through a polling sensor. If no sensor exists (e.g. `create_sensor: false`), observe mode auto-creates one — using a mesh-aware sensor when excluded packages are present.

This addresses a common adoption pattern where customers want Dagster for visibility and lineage before handing it orchestration control.

### Source metadata enabled

Added `enable_source_metadata: true` to `translation_settings` in both YAML configs. Without this, dbt sources (e.g. bronze tables) don't get metadata on their `AssetDep` objects, which prevents upstream asset key remapping and breaks bronze-to-silver lineage. This setting defaults to `False` in dagster-dbt, so it must be set explicitly.

### dbt selection by name enabled

Added `enable_dbt_selection_by_name: true` to `translation_settings` in both YAML configs. In orchestrate mode with dbt mesh, cross-project FQNs include the package name (e.g. `silver_project.staging.customers`), which doesn't match the remote dbt Cloud project's namespace. This setting uses the file name stem instead, ensuring subset materialization works correctly across mesh boundaries. Harmless in observe mode.

### Automatic sensor creation in observe mode

When `mode: observe` is set and no sensor was created by the base component, the `DbtCloudMeshComponent` automatically creates one:
- **With excluded packages**: Creates a mesh-aware sensor that filters out materialization events for external package models
- **Without excluded packages**: Creates a simple observe sensor

### Auto-refresh sensor for dbt Cloud changes (Experimental)

Added `dbt_cloud_state_refresh_sensor` that monitors dbt Cloud workspaces for actual project structure changes. Uses manifest fingerprinting (node IDs + file checksums) to distinguish real changes from routine runs -- only triggers a refresh when the project structure changed (new models, modified SQL, schema updates). Uses `dg plus deploy refresh-defs-state` with forward-compatible `--defs-state-key` targeting (falls back to full refresh until the flag is added to the cloud command). After refreshing state, reloads the code location via the Dagster Cloud GraphQL API so the running code server picks up the new definitions.

### Test coverage

Added `TestObserveMode` test class covering:
- `AssetsDefinition` to `AssetSpec` conversion
- Cross-project dependency preservation through observe conversion
- Automatic sensor creation (standard and mesh-aware)
- Default mode backwards compatibility
