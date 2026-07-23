# Spotify End-to-End Data Engineering Project

An end-to-end lakehouse pipeline on Azure that ingests Spotify data from a relational source, lands it through a medallion architecture (bronze → silver → gold), and governs it with Unity Catalog — built to mirror how a real enterprise data platform is designed, not a single-notebook demo.

## Architecture

```
                     ┌───────────────────────────────────────────┐
                     │              Azure Data Factory             │
                     │                                             │
  Azure SQL DB  ───▶ │  Control/Metadata Table (per-source config) │
  (source data)      │            │                                │
                     │            ▼                                │
                     │      ForEach (per table)                    │
                     │            │                                │
                     │            ▼                                │
                     │   Switch (Load Type)                        │
                     │   ├── Incremental (watermark-based CDC)     │
                     │   ├── Full Load                             │
                     │   └── Backfill (watermark NOT updated)      │
                     │            │                                │
                     │            ▼                                │
                     │   On Failure ──▶ Web Activity ──▶ Logic App │
                     │                                  ──▶ Email  │
                     └───────────────┬─────────────────────────────┘
                                      ▼
                     ┌───────────────────────────────────────────┐
                     │        ADLS Gen2 — Bronze Container         │
                     │     (raw Parquet, idempotent sink paths)    │
                     └───────────────┬─────────────────────────────┘
                                      ▼
                     ┌───────────────────────────────────────────┐
                     │         Azure Databricks (DAB deploy)       │
                     │                                             │
                     │  Auto Loader (cloudFiles) ── streaming ──▶  │
                     │  Bronze Delta Tables                        │
                     │            │                                │
                     │            ▼  (Delta Live Tables)           │
                     │  Silver: cleansing, dedup, schema            │
                     │          enforcement, DQ expectations        │
                     │            │                                │
                     │            ▼                                │
                     │  Gold: business aggregates                  │
                     │        (top artists/tracks, trends)         │
                     │                                             │
                     │  Governed end-to-end by Unity Catalog        │
                     │  (Storage Credential → External Location     │
                     │   → Metastore → Catalog/Schema/Table ACLs)   │
                     └───────────────────────────────────────────┘
```

## Layer-by-Layer Breakdown

### 1. Ingestion — Azure Data Factory

A single **metadata-driven pipeline** replaces the anti-pattern of one pipeline per table:

- A **control table** in Azure SQL DB stores per-source config: table name, load type, watermark column, and last watermark value.
- A **ForEach** activity iterates over every entry in the control table, so onboarding a new source table is a metadata insert, not a pipeline change.
- A **Switch** activity branches on load type:
  - **Incremental** — pulls only rows newer than the stored watermark (watermark-based CDC).
  - **Full Load** — pulls the entire table; used for dimension/reference tables or first-time loads.
  - **Backfill** — reprocesses a historical date range on demand. Critically, backfill runs **do not update the watermark**, so a later incremental run can't skip rows that fall between the old and new watermark values. This was a deliberate fix for a subtle correctness bug in naive incremental designs.
- **Idempotent sink paths** — bronze landing paths in ADLS Gen2 are deterministic by table/load-type/date, so reruns overwrite instead of duplicating data.
- **Failure alerting** — a Web Activity calls a Logic App that sends an email notification on pipeline failure, adding basic operational visibility.

### 2. Storage — ADLS Gen2

- **Bronze container**: raw landing zone for Parquet files delivered by ADF, partitioned by table and date.
- **Silver/Gold**: Delta-formatted, versioned storage managed through Unity Catalog external locations rather than raw file paths.

Access is governed by a **Storage Credential** (managed identity / service principal) linked to an **External Location**, which is in turn linked to the Unity Catalog metastore root. Breaking this chain is what causes errors like `DAC_DOES_NOT_EXIST` — a good example of understanding UC's security model rather than treating it as a black box.

### 3. Processing — Azure Databricks

- **Bronze ingestion**: **Auto Loader** (`cloudFiles`) incrementally and idempotently streams new Parquet files from the bronze container into Delta tables, with automatic schema inference/evolution and checkpoint-based tracking so reruns don't reprocess files already ingested.
- **Silver layer**: built with **Delta Live Tables (DLT)** — declarative pipeline definitions with built-in data quality expectations (constraints that can drop, flag, or fail records that don't meet quality rules). Handles cleansing, deduplication, and schema conformance.
- **Gold layer**: business-level aggregates and modeled tables ready for BI/analytics consumption (e.g., top artists/tracks, listening trend metrics).
- **Unity Catalog** governs the entire layer centrally — catalog/schema/table-level permissions, lineage, and audit, replacing workspace-scoped Hive metastore permissions.
- **Deployment**: the pipeline and job definitions are packaged as a **Databricks Asset Bundle (DAB)** — infrastructure-as-code for Databricks, deployable via CLI/CI rather than clicked together manually in the UI. This makes the environment reproducible and version-controlled.

## Design Themes

- **Config-driven ingestion** (control table + ForEach/Switch) — scales to new sources without pipeline changes.
- **Idempotency and correctness under reruns** — deterministic sink paths, and backfill logic that protects the watermark from corruption.
- **Streaming-capable, governed lakehouse** — Auto Loader + DLT + Unity Catalog instead of ad hoc batch drops.
- **Infrastructure as code** — Databricks Asset Bundle for reproducible, deployable pipeline definitions.
- **Operational visibility** — automated failure alerting via Logic App.

## Challenges & Design Decisions

**Problem: nested conditionals in ADF don't scale.**
The original design tried to handle Incremental / Full / Backfill logic as branches nested inside a single If Condition activity. ADF's If Condition doesn't support cleanly nesting another If Condition inside its True/False branches — it's technically possible through the JSON, but it quickly becomes unmanageable to debug, extend, or reason about once watermark logic is layered in on top.

**Problem: updating the watermark via a JSON file is fragile.**
An early version tracked the watermark value in a JSON file, updated in-place mid-pipeline. This has no concurrency safety, no audit trail, is hard to query or validate, and is easy to corrupt on a failed or retried run — not an industry-standard pattern for CDC.

**Resolution:**
- Split **backfill into its own standalone pipeline** instead of nesting it as a conditional branch inside the main pipeline. Each pipeline now has a single, clear responsibility, which is easier to debug, test, and maintain.
- Replaced the JSON-file watermark with a **SQL control/metadata table** as the single source of truth — queryable, auditable, and safely updatable via a Lookup/Script activity or stored procedure, matching how production CDC pipelines actually track state.
- This is also why backfill runs are wired to **never update the watermark table** (see Ingestion Layer above) — once the watermark lived in a proper control table, it became straightforward to gate which pipelines are allowed to write to it.

This iteration — hit a real platform limitation, understood *why* it broke, redesigned around it — is arguably a better story than a first-draft pipeline that happened to work.

## Repository Structure

```
├── factory/          # ADF pipeline definitions
├── pipeline/          # ADF pipeline JSON
├── linkedService/      # ADF linked service configs
├── dataset/           # ADF dataset definitions
├── databricks/
│   └── .bundle/spotify_dab/   # Databricks Asset Bundle (DLT pipelines, jobs)
└── publish_config.json
```

## Tech Stack

Azure Data Factory · Azure SQL DB · ADLS Gen2 · Azure Databricks · Delta Lake · Delta Live Tables · Unity Catalog · Auto Loader · Databricks Asset Bundles · Logic Apps
