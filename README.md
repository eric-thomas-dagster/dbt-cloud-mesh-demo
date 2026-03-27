# dbt Cloud Mesh Demo

Dagster project demonstrating orchestration of two **dbt Cloud** projects that use dbt mesh for cross-project references. One project handles bronze-to-silver transformations, the other handles silver-to-gold and references silver models via dbt mesh.

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
uv run dg utils refresh-defs-state
```

### 6. Run Dagster

```bash
uv run dg dev
```

Open http://localhost:3000 to see the asset graph.

## Running with Mock State (No dbt Cloud Required)

This project ships with pre-baked state files so you can explore the Dagster UI without dbt Cloud credentials:

```bash
export DBT_CLOUD_TOKEN=placeholder
uv run dg dev
```

The asset graph, lineage, and groups will render correctly. Materialization and the polling sensor will not function without real credentials.
