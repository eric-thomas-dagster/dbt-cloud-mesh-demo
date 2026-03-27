# dbt Cloud Mesh Demo

Dagster project demonstrating orchestration of two **dbt Cloud** projects that use dbt mesh for cross-project references. A custom `DbtCloudMeshComponent` extends `DbtCloudComponent` with cross-project lineage and group override support.

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

- **`external_packages`**: Creates external asset specs for public models from other dbt projects, preserving cross-project lineage without duplicating assets.
- **`group_overrides`**: Maps dbt resource types or fqn path segments to Dagster groups via YAML, even when the dbt project doesn't define groups. Uses a custom `DagsterDbtTranslator` under the hood.
- **`create_sensor`**: When `true`, polls dbt Cloud for runs triggered outside of Dagster and emits materialization events into the asset graph.

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

Edit `src/dbt_cloud_mesh_demo/defs/silver_cloud/defs.yaml`:

```yaml
attributes:
  workspace:
    account_id: YOUR_ACCOUNT_ID      # <-- replace
    token: "{{ env.DBT_CLOUD_TOKEN }}"
    access_url: "https://cloud.getdbt.com"
    project_id: YOUR_SILVER_PROJECT_ID   # <-- replace
    environment_id: YOUR_SILVER_ENV_ID   # <-- replace
```

Edit `src/dbt_cloud_mesh_demo/defs/gold_cloud/defs.yaml` similarly with the gold project/environment IDs.

### 4. Enable the polling sensor

Once connected to real dbt Cloud, enable the sensor by changing `create_sensor` to `true` in both `defs.yaml` files:

```yaml
  create_sensor: true
```

### 5. Refresh the dbt Cloud state

Remove the pre-baked mock state and refresh from dbt Cloud:

```bash
rm -rf src/dbt_cloud_mesh_demo/defs/.local_defs_state/DbtCloudComponent__*
```

Also set `refresh_if_dev: true` in the `defs_state` block of each `defs.yaml` (or remove the `defs_state` block entirely to use the default):

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

## How the Polling Sensor Works

When `create_sensor: true` and real credentials are configured, the `DbtCloudComponent` creates a sensor that:

1. Polls dbt Cloud every 30 seconds for completed runs
2. Filters out runs triggered by Dagster itself (adhoc jobs)
3. Emits **materialization events** for each dbt model that ran externally
4. Updates the Dagster asset graph with the latest run metadata

This means when someone runs a dbt Cloud job via the dbt Cloud UI, CI/CD, or the dbt Cloud scheduler, Dagster automatically picks up the results and reflects them in the asset catalog.
