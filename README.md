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

## How dbt Mesh Works with Dagster

When two dbt projects use dbt mesh, the downstream project (gold) pulls in upstream models (silver) via `packages.yml`. This means the gold project's manifest contains **both** gold and silver models. Dagster needs to handle this correctly to avoid duplicate assets while preserving lineage.

The key insight: **`exclude: "package:silver_project"` on the gold component prevents duplicate assets, and the dependency edges from gold models to silver asset keys are preserved.** Dagster resolves those dependencies to the real silver assets regardless of whether they live in the same or different code locations.

## Single Code Location (Recommended)

If both dbt Cloud projects live in the **same Dagster code location**, use the standard `DbtCloudComponent` with no custom code:

```yaml
# silver_cloud/defs.yaml
type: dagster_dbt.DbtCloudComponent
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
type: dagster_dbt.DbtCloudComponent
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
- **Lineage is preserved.** `exclude` only prevents silver models from becoming assets in the gold component - it does **not** strip the dependency edges. Gold models like `customer_360` still have `deps=[AssetKey(["customers"])]`, which resolves to the silver component's real asset.
- **The polling sensor just works.** When the gold sensor emits materializations for silver models (included in run results via dbt packages), those asset keys exist in the code location's asset graph from the silver component, so the sensor's toposort lookup succeeds.

No custom component, no external assets, no sensor filtering needed.

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

This project includes a `DbtCloudMeshComponent` that solves the sensor problem for multi-code-location deployments by replacing the standard polling sensor with a **mesh-aware sensor** that filters out materialization events for models belonging to external packages.

```yaml
# gold_cloud/defs.yaml (multi-code-location)
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

The `external_packages` config tells the mesh-aware sensor which packages to filter. The silver code location's own sensor handles silver model materializations independently.

### Additional Feature: group_overrides

The `DbtCloudMeshComponent` also supports `group_overrides` for assigning Dagster groups from YAML when the dbt project doesn't define them:

```yaml
  group_overrides:
    seed: raw
    staging: bronze
    silver: silver
    gold: gold
```

Matches by resource type first (e.g. `seed`), then by fqn path segment (e.g. `staging`). Falls back to the dbt-defined group if no override matches.

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

## Auto-Refreshing Definitions on dbt Cloud Changes

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

Dagster Cloud automatically provides `DAGSTER_CLOUD_DEPLOYMENT_NAME` and `DAGSTER_CLOUD_ORGANIZATION` -- the `dg plus deploy refresh-defs-state` CLI reads both from the environment, so no extra configuration is needed for the refresh command.

Set the dbt Cloud environment variables (shared with workspace components):

```bash
export DBT_CLOUD_ACCOUNT_ID="12345"
export DBT_CLOUD_TOKEN="your-token"

# Per-workspace project/environment IDs
export DBT_CLOUD_SILVER_PROJECT_ID="100001"
export DBT_CLOUD_SILVER_ENVIRONMENT_ID="200001"
export DBT_CLOUD_GOLD_PROJECT_ID="100002"
export DBT_CLOUD_GOLD_ENVIRONMENT_ID="200002"
```

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

### Auto-refresh sensor for dbt Cloud changes

Added `dbt_cloud_state_refresh_sensor` that monitors dbt Cloud workspaces for actual project structure changes. Uses manifest fingerprinting (node IDs + file checksums) to distinguish real changes from routine runs -- only triggers a refresh when the project structure changed (new models, modified SQL, schema updates). Uses `dg plus deploy refresh-defs-state` with forward-compatible `--defs-state-key` targeting (falls back to full refresh until the flag is added to the cloud command). After refreshing state, reloads the code location via the Dagster Cloud GraphQL API so the running code server picks up the new definitions.

### Test coverage

Added `TestObserveMode` test class covering:
- `AssetsDefinition` to `AssetSpec` conversion
- Cross-project dependency preservation through observe conversion
- Automatic sensor creation (standard and mesh-aware)
- Default mode backwards compatibility
