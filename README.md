# dbt Cloud Mesh Demo

Dagster project demonstrating orchestration of two **dbt Cloud** projects that use dbt mesh for cross-project references.

## Single Code Location (Recommended)

If both dbt Cloud projects live in the **same Dagster code location**, you can use the standard `DbtCloudComponent` with no custom code at all:

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

- **Asset keys are globally unique** within a code location. The silver component creates assets for silver models, and the gold component's `exclude` prevents duplicating them.
- **Cross-project lineage resolves automatically.** Gold models depend on silver asset keys that already exist in the asset graph from the silver component.
- **The polling sensor just works.** When the gold sensor emits materializations for silver models (included in the run results via dbt packages), those asset keys exist in the code location's asset graph, so the sensor's toposort lookup succeeds.

No custom component, no external assets, no sensor filtering needed.

## Multi-Code Location Deployments

If your silver and gold projects are in **separate code locations** (separate Dagster projects or deployments), the standard `DbtCloudComponent` sensor will fail with an error like:

```
AssetKey(['silver', 'base_exposure_relationships']) is not in list
```

This happens because the sensor's `sorted_asset_events` function looks up asset keys in `repository_def.asset_graph`, which is scoped to the **current code location**. Silver assets don't exist in the gold code location's graph.

For this scenario, this project includes a `DbtCloudMeshComponent` that solves the problem.

## Architecture

```
src/dbt_cloud_mesh_demo/
  components/
    dbt_cloud_mesh_component.py   Custom DbtCloudMeshComponent (subclasses DbtCloudComponent)
  defs/
    silver_cloud/                 Component instance for the silver dbt Cloud project
    gold_cloud/                   Component instance for the gold dbt Cloud project
```

### DbtCloudMeshComponent Features

- **`external_packages`**: Creates external asset specs for public models from other dbt projects, preserving cross-project lineage without duplicating assets. Also tells the mesh-aware sensor to skip those models.
- **`group_overrides`**: Maps dbt resource types or fqn path segments to Dagster groups via YAML, even when the dbt project doesn't define groups. Uses a custom `DagsterDbtTranslator` under the hood.
- **Mesh-aware sensor**: Replaces the standard polling sensor with one that filters out materialization events for external package models, preventing the "not in list" error in multi-code-location setups.

### How the Mesh-Aware Sensor Works

In a dbt mesh setup, the gold project's manifest includes models from the silver project (pulled in via `packages.yml`). When a gold dbt Cloud run completes, the run results contain **both** gold and silver models. In a multi-code-location deployment, the silver asset keys don't exist in the gold code location's asset graph.

The `DbtCloudMeshComponent` replaces the standard sensor with a mesh-aware sensor that:

1. Identifies all node IDs belonging to packages listed in `external_packages`
2. Filters out materialization events for those nodes before emitting them
3. Only emits events for models that are actually **owned** by this code location

The silver code location's own sensor handles silver model materializations independently.

### How External Assets Work

When the gold dbt project references silver models via `{{ ref('silver_project', 'customers') }}`, those silver models appear in gold's manifest as nodes with `package_name: "silver_project"`. In a multi-code-location setup, the `DbtCloudMeshComponent` handles this with:

1. **`exclude: "package:silver_project"`** - Prevents silver models from being created as regular dbt assets, avoiding duplication with the silver code location.
2. **`external_packages`** - Generates `AssetSpec` objects for **public** silver models (respecting dbt's `access: public` modifier). These appear as external assets in the gold code location's asset graph, preserving cross-project dependency lineage.

In a single code location, neither of these features is needed - the lineage resolves automatically.

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
