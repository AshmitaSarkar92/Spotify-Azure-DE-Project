# Spotify Azure Data Engineering Project

![Azure Data Factory](https://img.shields.io/badge/Azure_Data_Factory-0078D4?style=flat&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=flat&logo=delta&logoColor=white)
![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-00A972?style=flat)
![Azure SQL](https://img.shields.io/badge/Azure_SQL-CC2927?style=flat&logo=microsoftsqlserver&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Jinja2](https://img.shields.io/badge/Jinja2-B41717?style=flat&logo=jinja&logoColor=white)

An end-to-end Azure data platform built around a simulated Spotify dataset — ingestion with Azure Data Factory, incremental Bronze/Silver/Gold layering on ADLS Gen2, metadata-driven transformations with Jinja2, and a Slowly Changing Dimension (SCD Type 2) star schema built with Delta Live Tables on Databricks, deployed via Databricks Asset Bundles.

## Project Overview

### The Problem

Data isn't static new rows show up and old ones change, every day. A pipeline that reprocesses everything on every run, or breaks the moment the source adds a column, doesn't hold up once it's actually running.

### The Solution

Process only what changed. Describe tables as metadata instead of hardcoding them. Keep history instead of overwriting it.

| Technique | First principle |
|---|---|
| **Parameterized ADF pipeline** | One pipeline, driven by metadata — a new table is config, not code |
| **Watermark-based CDC** | Only pull what changed since last run, never the whole table |
| **Jinja2-parameterized SQL** | Describe a table once as data; let it generate its own SQL |
| **Autoloader (`cloudFiles`)** | Process each file once; adapt to new columns automatically |
| **Deduplication** | Streams can redeliver — never trust "once" without checking |
| **SCD Type 2** | Keep every version of a row, not just the latest one |
| **Keyless governance** | Identity grants access — no keys to leak |
| **Run monitoring** | A failure you don't hear about isn't handled |

Same rule everywhere: **the pipeline changes when the data changes — never when new data simply arrives.**

### Why This Stands Out

Every technique here was chosen to make the pipeline more efficient and more reliable, not to check a box. Incremental processing instead of reprocessing everything cuts runtime and cost as the dataset grows. Metadata-driven pipelines and SQL mean new sources are a config change, not new code. Managed-identity governance removes an entire class of credential risk. SCD Type 2 keeps history that would otherwise be lost on every overwrite. Run monitoring means a failure is caught the same day, not discovered weeks later. Together, these are the choices that keep a pipeline fast, safe, and maintainable as it scales — not just working once, but working well over time.

## Architecture Overview

![Pipeline Architecture](screenshots/spotify_adf_ss/00-pipeline-architecture-flowchart.png)

All resources live in a single resource group (`RG-SpotifyProject`):

![Resource Group Overview](screenshots/spotify_adf_ss/01-resource-group-overview.png)

| Resource | Purpose |
|---|---|
| `adf-spotifyproject-ash` | Azure Data Factory — incremental ingestion from Azure SQL into the data lake |
| `spotifyprojectdatalake` | ADLS Gen2 storage account — Bronze / Silver / Gold containers |
| `spotify_adb` | Azure Databricks workspace — Silver/Gold transformations, Unity Catalog, DLT |
| `azurespotifyprojserver` / `azurespotifyprojectdb` | Azure SQL — source database |
| `access_spotify` | Access Connector for Azure Databricks — lets Unity Catalog read/write the data lake via managed identity |
| `logicappspotifyproj` | Logic App — email alerting, triggered by the ADF pipeline |

**Data flow:**

```
Azure SQL (source)
      │  incremental CDC pull (ForEach loop pipeline)
      ▼
Bronze (ADLS Gen2, raw parquet)
      │  Databricks Autoloader (schema evolution + checkpointing)
      ▼
Silver (ADLS Gen2, cleaned Delta tables)
      │  Jinja2 metadata-driven SQL + Delta Live Tables
      ▼
Gold (Unity Catalog managed Delta tables, SCD Type 2 star schema)
```

---

## 1. Ingestion — Azure Data Factory

A single parameter-driven `Incremental_Loop` pipeline handles every dimension/fact table via a `ForEach` activity over a `loop_array` parameter — adding a new source table is a metadata change, not a new pipeline.

| Step | Screenshot |
|---|---|
| Pipeline canvas (ForEach + Web alert) | ![Pipeline Canvas](screenshots/spotify_adf_ss/02-pipeline-canvas-overview.png) |
| ForEach loop configuration | ![ForEach General](screenshots/spotify_adf_ss/03-foreach-loop-general.png) |
| ForEach activities list | ![ForEach Activities](screenshots/spotify_adf_ss/04-foreach-loop-activities-list.png) |
| `last_cdc` Lookup — reads last watermark from `cdc.json` | ![Lookup last_cdc](screenshots/spotify_adf_ss/05-lookup-last-cdc-activity.png) |
| Copy Data — Source (parameterized SQL query) | ![Copy Data Source](screenshots/spotify_adf_ss/06-copydata-source-query.png) |
| Copy Data — Sink (dynamic path into `bronze`) | ![Copy Data Sink](screenshots/spotify_adf_ss/07-copydata-sink-bronze-path.png) |
| If Condition — update watermark or clean up empty files | ![If Condition](screenshots/spotify_adf_ss/08-ifcondition-branches.png) |
| Web activity — POSTs to Logic App for email alerts | ![Web Alert Settings](screenshots/spotify_adf_ss/09-web-alert-activity-settings.png) |
| `Incremental_Ingestion` pipeline (full pipeline, non-loop variant) | ![Incremental Ingestion](screenshots/spotify_adf_ss/10-incremental-ingestion-pipeline.png) |
| `Incremental_Loop` pipeline (full view) | ![Incremental Loop Full](screenshots/spotify_adf_ss/11-incremental-loop-pipeline-full.png) |
| Dataset — `azure_sql` source | ![Dataset Azure SQL](screenshots/spotify_adf_ss/12-dataset-azure-sql.png) |
| Dataset — `cdc_json_dynamic` (watermark tracking) | ![Dataset CDC JSON](screenshots/spotify_adf_ss/13-dataset-cdc-json-dynamic.png) |
| Dataset — `parquet_file_dynamic` (Bronze sink) | ![Dataset Parquet](screenshots/spotify_adf_ss/14-dataset-parquet-file-dynamic.png) |
| Successful pipeline run — `Incremental_Ingestion` | ![Run Ingestion Success](screenshots/spotify_adf_ss/15-pipeline-run-ingestion-success.png) |
| Successful pipeline run — `Incremental_Loop` (debug) | ![Run Loop Debug](screenshots/spotify_adf_ss/16-pipeline-run-loop-debug-success.png) |
| Successful pipeline run — `Incremental_Loop` | ![Run Loop Success](screenshots/spotify_adf_ss/17-pipeline-run-loop-success.png) |

**Key pattern:** a `Lookup` reads the last-processed timestamp from a `cdc.json` watermark file, a parameterized `Copy Data` activity pulls only new/changed rows via SQL, and an `If Condition` either advances the watermark (incremental data found) or deletes the empty output file (nothing new). A `Web` activity posts pipeline status to a Logic App, which emails an alert.

---

## 2. Storage — ADLS Gen2 (Bronze / Silver / Gold)

| Step | Screenshot |
|---|---|
| Storage account IAM — Owner + Managed Identity roles | ![Storage IAM](screenshots/spotify_datalake_ss/01-storage-account-iam.png) |
| `bronze` container — raw landed data per table | ![Bronze Listing](screenshots/spotify_datalake_ss/02-bronze-container-listing.png) |
| `bronze/DimArtist` — incremental parquet files | ![Bronze DimArtist](screenshots/spotify_datalake_ss/03-bronze-dimartist-files.png) |
| `bronze/DimArtist_cdc` — watermark (`cdc.json`) tracking | ![Bronze CDC Tracking](screenshots/spotify_datalake_ss/04-bronze-dimartist-cdc-tracking.png) |
| `silver` container — cleaned Delta tables | ![Silver Listing](screenshots/spotify_datalake_ss/05-silver-container-listing.png) |
| `silver/DimArtist` — `checkpoint` + `data` subfolders | ![Silver Checkpoint/Data](screenshots/spotify_datalake_ss/06-silver-dimartist-checkpoint-data-folders.png) |
| Autoloader checkpoint internals (schema tracking, offsets, commits) | ![Autoloader Checkpoint](screenshots/spotify_datalake_ss/07-silver-autoloader-checkpoint-state.png) |
| Silver Delta table data files | ![Silver Delta Files](screenshots/spotify_datalake_ss/08-silver-delta-data-files.png) |
| Silver `_delta_log` transaction log | ![Delta Log](screenshots/spotify_datalake_ss/09-silver-delta-log-folder.png) |
| `gold` container — only Unity Catalog's internal `__unitystorage` folder | ![Gold Unitystorage](screenshots/spotify_datalake_ss/10-gold-container-unitystorage.png) |
| `__unitystorage` managed table internals | ![Unitystorage Tables](screenshots/spotify_datalake_ss/11-gold-unitystorage-managed-tables.png) |

**Note on Gold storage:** Gold tables are Unity Catalog **managed tables** — created without an explicit path — so their physical files live under `gold/__unitystorage/catalogs/<catalog-id>/.../tables/<table-id>/`, organized by internal IDs rather than table names. This is normal Unity Catalog behavior and doesn't conflict with anything written to an explicit path.

---

## 3. Unity Catalog & Access

| Step | Screenshot |
|---|---|
| External Locations — `bronze`/`silver`/`gold` mapped with credentials | ![External Locations](screenshots/spotify_unitycatalog_ss/01-external-locations-bronze-silver-gold.png) |
| Access Connector for Azure Databricks (managed identity) | ![Access Connector](screenshots/spotify_access_connector_ss/01-access-connector-overview.png) |

Unity Catalog reaches the data lake through a dedicated **Access Connector** (a managed identity), granted `Storage Blob Data Contributor` on the storage account — no storage keys are embedded in notebooks or pipelines.

---

## 4. Alerting — Logic App

| Step | Screenshot |
|---|---|
| Logic App overview + run history | ![Logic App Overview](screenshots/spotify_logicapp_ss/01-logic-app-overview-run-history.png) |
| Designer flow — HTTP trigger → Send email | ![Logic App Designer](screenshots/spotify_logicapp_ss/02-logic-app-designer-flow.png) |

The ADF pipeline's `Web` activity posts the pipeline name and status to this Logic App's HTTP trigger, which sends an email notification — simple, serverless pipeline monitoring without extra infrastructure.

---

## 5. Databricks — Silver & Gold Transformations

Project structure (Databricks Asset Bundle `spotify_dab`):

```
spotify_dab/
  databricks.yml          # bundle config (dev/prod targets)
  resources/               # job & pipeline definitions
  src/
    silver/
      Silver_Dimensions     # Autoloader notebook
    gold/
      Gold_Pipeline/
        transformations/    # DLT notebooks (DimArtist, DimDate, DimTrack, DimUser, FactStream)
    Jinja/
      jinja_notebook         # metadata-driven SQL generation
  utils/
    transformations.py      # reusable helper class
```

| Step | Screenshot |
|---|---|
| Workspace root — `spotify_dab`, `.bundle` | ![Workspace Root](screenshots/spotify_databricks_ss/01-workspace-root-folders.png) |
| `spotify_dab` folder listing | ![Spotify DAB Listing](screenshots/spotify_databricks_ss/02-spotify-dab-folder-listing.png) |
| `databricks.yml` — dev/prod targets | ![databricks.yml](screenshots/spotify_databricks_ss/03-databricks-yml-targets.png) |
| Tree view — Gold_Pipeline transformations | ![Gold Tree View](screenshots/spotify_databricks_ss/04-tree-view-gold-pipeline-transformations.png) |
| Tree view — Silver notebook + utils + config | ![Silver/Utils Tree View](screenshots/spotify_databricks_ss/05-tree-view-silver-utils-config-files.png) |

### 5a. Silver Layer — Autoloader

| Step | Screenshot |
|---|---|
| Notebook setup (imports, custom `utils.transformations`) | ![Silver Setup](screenshots/spotify_databricks_ss/06-silver-notebook-setup-cells.png) |
| `readStream` with Autoloader (`cloudFiles`) — schema evolution + checkpointing | ![Autoloader readStream](screenshots/spotify_databricks_ss/07-autoloader-readstream-output.png) |
| Transformations — uppercase `user_name`, drop `_rescued_data`, dedupe on `user_id` | ![DimUser Transformations](screenshots/spotify_databricks_ss/07a-silver-dimuser-transformations-dedup.png) |
| `writeStream` — append to Delta, `trigger(once=True)`, registered to Unity Catalog | ![DimUser writeStream](screenshots/spotify_databricks_ss/07b-silver-dimuser-writestream.png) |

Autoloader (`cloudFiles` format) streams new Bronze parquet files into Silver, tracking processed files via a checkpoint directory (exactly-once processing) and automatically evolving the schema as new columns appear. Each dimension is cleaned (column standardization, dropping Autoloader's `_rescued_data`, deduplication) and then written out with `trigger(once=True)` to a managed Silver Delta table — the same pattern repeats for `DimArtist`, `DimTrack`, `DimDate`, and `FactStream`.

### 5b. Reusable Utilities

| Step | Screenshot |
|---|---|
| `transformations.py` — `dropColumns`, `previewStream` | ![Utils 1](screenshots/spotify_databricks_ss/08-transformations-dropcolumns-previewstream.png) |
| `transformations.py` — `writeStream`/`clearPreview` | ![Utils 2](screenshots/spotify_databricks_ss/09-transformations-writestream-clearpreview.png) |

A small reusable class, imported across notebooks, to avoid repeating the same streaming boilerplate (preview a stream's output without committing it, clean up preview artifacts, drop unneeded columns).

### 5c. Metadata-Driven SQL with Jinja2

| Step | Screenshot |
|---|---|
| Metadata `parameters` list — FactStream/DimUser (with join condition) | ![Jinja Parameters 1](screenshots/spotify_databricks_ss/10-jinja-parameters-list-factstream-dimuser.png) |
| Metadata `parameters` list — DimTrack | ![Jinja Parameters 2](screenshots/spotify_databricks_ss/11-jinja-parameters-list-dimtrack.png) |
| Jinja2 SQL template — SELECT column loop | ![Jinja Template 1](screenshots/spotify_databricks_ss/12-jinja-sql-template-select-columns.png) |
| Jinja2 SQL template — FROM/JOIN loop + rendered output | ![Jinja Template 2](screenshots/spotify_databricks_ss/13-jinja-sql-template-join-render-output.png) |

Each output table is described as **data** (a dictionary: source table, columns, join, condition), and a Jinja2 template renders that metadata into real SQL at runtime — the SELECT column list and the FROM/JOIN clause are built with two separate loops so the base table is never mistakenly rendered with `LEFT JOIN` syntax. Adding a new Silver table becomes a metadata change, not new code — mirroring the ADF `ForEach`/`loop_array` pattern.

### 5d. Gold Layer — Delta Live Tables (SCD Type 2)

| Step | Screenshot |
|---|---|
| `DimArtist` — `create_auto_cdc_flow`, SCD type 2 | ![DimArtist DLT](screenshots/spotify_databricks_ss/14-dimartist-dlt-auto-cdc-scd2.png) |
| `DimUser` — data quality expectations (`expect_all_or_drop`) | ![DimUser Expectations](screenshots/spotify_databricks_ss/15-dimuser-dlt-expectations.png) |
| `DimUser` — `create_auto_cdc_flow`, SCD type 2 | ![DimUser DLT](screenshots/spotify_databricks_ss/16-dimuser-dlt-auto-cdc-scd2.png) |
| Completed `Gold_Pipeline` run — all 10 tables succeeded | ![Gold Pipeline Run](screenshots/spotify_databricks_ss/17-gold-pipeline-dag-completed-run.png) |

Each dimension uses `dlt.create_auto_cdc_flow(..., stored_as_scd_type="2")` to automatically track full history: a new row is inserted whenever source data changes, with `__START_AT`/`__END_AT` columns marking each version's validity window. `DimUser` additionally enforces a data-quality rule (`user_id IS NOT NULL`) via DLT expectations, dropping any row that fails.

### 5e. Final Gold Tables & SCD2 Proof

| Step | Screenshot |
|---|---|
| Silver `dimartist` sample data | ![Silver DimArtist Sample](screenshots/spotify_databricks_ss/18-silver-dimartist-sample-data.png) |
| Silver `dimuser` sample data | ![Silver DimUser Sample](screenshots/spotify_databricks_ss/19-silver-dimuser-sample-data.png) |
| Gold `dimartist` sample data (10 Gold tables total) | ![Gold DimArtist Sample](screenshots/spotify_databricks_ss/20-gold-dimartist-sample-data.png) |
| Gold `dimartist_stg` (DLT staging table) sample data | ![Gold DimArtist Staging](screenshots/spotify_databricks_ss/21-gold-dimartist-stg-sample-data.png) |
| Query — rows still open (`__END_AT IS NULL`) | ![SCD2 Open Rows](screenshots/spotify_databricks_ss/22-scd2-query-end-at-not-null.png) |
| Query — same `track_id` appearing multiple times (history) | ![SCD2 History](screenshots/spotify_databricks_ss/23-scd2-query-track-id-history.png) |
| `__START_AT` / `__END_AT` versioning in detail | ![SCD2 Versioning](screenshots/spotify_databricks_ss/24-scd2-start-end-at-versioning.png) |

The last three queries confirm SCD Type 2 is genuinely working: the same `track_id` appears as multiple rows, each with a distinct `__START_AT`/`__END_AT` range — a clean, queryable history of every change rather than an overwritten "current state only" table.

---

## Tech Stack

- **Orchestration:** Azure Data Factory (parameterized ForEach pipelines, incremental CDC watermarking)
- **Storage:** Azure Data Lake Storage Gen2 (Bronze / Silver / Gold medallion architecture)
- **Compute:** Azure Databricks (Autoloader, Delta Live Tables, Unity Catalog)
- **Governance:** Unity Catalog with an Access Connector managed identity (no embedded storage keys)
- **Templating:** Jinja2 for metadata-driven SQL generation
- **Deployment:** Databricks Asset Bundles (`databricks.yml`, dev/prod targets)
- **Alerting:** Azure Logic Apps (HTTP trigger → email)
- **Source DB:** Azure SQL Database

## Repository Structure

```
├── azure-data-factory/       # ADF pipeline, dataset, and linked service JSON
├── source/                   # source data generation scripts
└── databricks/
    └── spotify_dab/           # Databricks Asset Bundle project (see section 5 above)
```
