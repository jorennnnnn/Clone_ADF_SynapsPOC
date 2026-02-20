# Azure Synapse Medallion ELT Framework

> **Metadata-driven, Copy-Activity-first ELT pipeline** replacing the original `sp_load_cetas_partition` stored-procedure approach with a fully orchestrated, scalable, idempotent pipeline framework on Azure Synapse Analytics (Serverless SQL pool).

---

## Table of Contents

1. [Current Logic Breakdown](#1-current-logic-breakdown)
2. [Identified Processing Patterns](#2-identified-processing-patterns)
3. [Target Metadata Model](#3-target-metadata-model)
4. [Pipeline Design Pattern](#4-pipeline-design-pattern)
5. [Copy Activity Configuration Strategy](#5-copy-activity-configuration-strategy)
6. [Incremental Load Strategy](#6-incremental-load-strategy)
7. [Error Handling & Logging Design](#7-error-handling--logging-design)
8. [End-to-End Execution Flow](#8-end-to-end-execution-flow)
9. [Migration Plan](#9-migration-plan)
10. [Risks & Optimisation Opportunities](#10-risks--optimisation-opportunities)

---

## 1. Current Logic Breakdown

### Stored Procedure: `[Bronze].[usp_load_cetas_partition]`

**File:** `sqlscript/sp_load_cetas_partition.json`

| Step | Description |
|------|-------------|
| **Parameters** | `@SourceConnectionName`, `@TableName`, `@PrimaryKey`, `@ColumnList`, `@ColumnTypeList`, `@WatermarkTS` (optional), `@DetectDeletes` (optional) |
| **RunId** | Generates a unique GUID suffix for the temporary external table name |
| **Partition path** | Constructs `/{SourceConnection}/{TableName}/loaddate={today}/` |
| **HashCompare** | When `@WatermarkTS IS NOT NULL`: LEFT JOINs the existing Bronze view (`bronze.vw_*`) to filter out records whose `row_hash` is unchanged since the last load |
| **DeleteCompare** | When `@DetectDeletes IS NOT NULL`: UNIONs with rows that exist in Bronze but are absent from the current Landing partition, writing them back with `IsDeleted = 1` |
| **CETAS execution** | Builds and executes a dynamic `CREATE EXTERNAL TABLE … AS SELECT` that reads OPENROWSET parquet from Landing, applies type casts, computes SHA2_256 row hash, and writes partitioned Parquet to Bronze |
| **Cleanup** | Drops the temporary external table from `sys.external_tables` after data is written |

**Key observations:**
- Entire flow is procedural T-SQL with dynamic SQL (`sp_executesql`)
- Parameters (column lists, types) are hardcoded at call-site (`sample_sprocCall.json`)
- No metadata store — configuration lives in the calling script
- No execution logging or error surfacing to a control table
- CETAS creates a temporary external table object as a write mechanism

---

## 2. Identified Processing Patterns

| Pattern | Original Implementation | Target Implementation |
|---------|------------------------|----------------------|
| **Type casting / schema enforcement** | Inline `CAST()` in dynamic SQL | Same CAST expressions stored in `ctrl.SourceTable.ColumnTypeList`, injected into Copy Activity source query |
| **Row hashing** | `HASHBYTES('SHA2_256', CONCAT_WS('|', ...))` in CETAS SELECT | Same expression in Serverless SQL source query of Copy Activity |
| **Change detection (hash compare)** | LEFT JOIN bronze view, filter on hash mismatch | Same LEFT JOIN, materialised via Copy Activity source query; controlled by watermark |
| **Delete detection** | UNION with Bronze rows absent from landing file | Second Copy Activity with `IsDeleted=1` guard; triggered by `DetectDeletes` flag in metadata |
| **Partition writing** | CETAS with `LOCATION = '.../loaddate={date}/'` | Copy Activity sink → ADLS Parquet with parameterised `folderPath` |
| **Idempotency** | CETAS: re-run creates a new object, old partition remains | Copy Activity: overwrite same folder path; watermark prevents re-processing |
| **Orchestration** | Manual stored-procedure call | `pl_master_orchestrator` ForEach over `ctrl.SourceTable` |
| **Logging** | `PRINT` statements only | `ctrl.PipelineExecution` table updated by child pipelines |

---

## 3. Target Metadata Model

All tables live in the **Azure SQL Database** (`metadata` database, `ctrl` schema).  
**Script:** `sqlscript/ctrl_create_metadata_tables.json`

### `ctrl.SourceTable` — Source table registry

```sql
CREATE TABLE ctrl.SourceTable (
    SourceTableId        INT            IDENTITY(1,1) PRIMARY KEY,
    SourceConnectionName NVARCHAR(200)  NOT NULL,   -- e.g. 'contoso'
    TableName            NVARCHAR(200)  NOT NULL,   -- e.g. 'customer'
    PrimaryKey           NVARCHAR(200)  NOT NULL,   -- e.g. 'CustomerKey'
    ColumnList           NVARCHAR(MAX)  NOT NULL,   -- '[col1],[col2],...' for CONCAT_WS hash
    ColumnTypeList       NVARCHAR(MAX)  NOT NULL,   -- 'CAST([col1] AS INT) AS col1,...'
    LoadType             NVARCHAR(20)   NOT NULL DEFAULT 'INCREMENTAL', -- FULL | INCREMENTAL
    IsActive             BIT            NOT NULL DEFAULT 1,
    DetectDeletes        BIT            NOT NULL DEFAULT 0,
    SilverEnabled        BIT            NOT NULL DEFAULT 0,
    GoldEnabled          BIT            NOT NULL DEFAULT 0,
    SilverQuery          NVARCHAR(MAX)  NULL,       -- override dedup query for Silver
    GoldQuery            NVARCHAR(MAX)  NULL,       -- custom aggregation/dimensional query for Gold
    CreatedDate          DATETIME2      NOT NULL DEFAULT GETDATE(),
    ModifiedDate         DATETIME2      NOT NULL DEFAULT GETDATE(),
    CONSTRAINT UQ_SourceTable UNIQUE (SourceConnectionName, TableName)
);
```

### `ctrl.Watermark` — Incremental load tracking

```sql
CREATE TABLE ctrl.Watermark (
    WatermarkId      INT       IDENTITY(1,1) PRIMARY KEY,
    SourceTableId    INT       NOT NULL REFERENCES ctrl.SourceTable(SourceTableId),
    LastLoadDate     DATE      NOT NULL,
    LastWatermarkTS  DATETIME2 NULL,
    UpdatedDate      DATETIME2 NOT NULL DEFAULT GETDATE(),
    CONSTRAINT UQ_Watermark UNIQUE (SourceTableId)
);
```

### `ctrl.PipelineExecution` — End-to-end audit log

```sql
CREATE TABLE ctrl.PipelineExecution (
    ExecutionId          INT            IDENTITY(1,1) PRIMARY KEY,
    PipelineName         NVARCHAR(200)  NOT NULL,
    RunId                NVARCHAR(100)  NOT NULL,   -- pipeline().runId (GUID)
    SourceTableId        INT            NULL,
    SourceConnectionName NVARCHAR(200)  NULL,
    TableName            NVARCHAR(200)  NULL,
    Layer                NVARCHAR(20)   NOT NULL,   -- BRONZE | SILVER | GOLD
    LoadDate             DATE           NULL,
    StartTime            DATETIME2      NOT NULL DEFAULT GETDATE(),
    EndTime              DATETIME2      NULL,
    Status               NVARCHAR(20)   NOT NULL DEFAULT 'RUNNING', -- RUNNING | SUCCESS | FAILED
    RowsRead             BIGINT         NULL,
    RowsWritten          BIGINT         NULL,
    ErrorMessage         NVARCHAR(MAX)  NULL
);
```

### Helper stored procedures

**Script:** `sqlscript/ctrl_stored_procedures.json`

| Procedure | Purpose |
|-----------|---------|
| `ctrl.usp_upsert_watermark` | MERGE watermark after successful Bronze load |
| `ctrl.usp_log_execution_start` | INSERT RUNNING row; returns `ExecutionId` |
| `ctrl.usp_log_execution_end` | UPDATE row with final status, row counts, and error |

---

## 4. Pipeline Design Pattern

### Folder: `medallion`

```
pl_master_orchestrator
 └── pl_landing_to_bronze     (one run per source table)
      └── pl_bronze_to_silver (when SilverEnabled = 1)
           └── pl_silver_to_gold (when GoldEnabled = 1)
```

| Pipeline | File | Responsibility |
|----------|------|----------------|
| `pl_master_orchestrator` | `pipeline/pl_master_orchestrator.json` | Reads `ctrl.SourceTable`, fans out via ForEach (parallelism = 5) |
| `pl_landing_to_bronze` | `pipeline/pl_landing_to_bronze.json` | Incremental / full load with hash compare and delete detection |
| `pl_bronze_to_silver` | `pipeline/pl_bronze_to_silver.json` | Dedup + active-record filter; optional custom Silver query |
| `pl_silver_to_gold` | `pipeline/pl_silver_to_gold.json` | Pass-through or custom aggregation/dimension model |

---

## 5. Copy Activity Configuration Strategy

### Bronze Copy Activity (`act_copy_inserts_updates`)

| Property | Value |
|----------|-------|
| **Source type** | `SqlDWSource` (Serverless SQL pool) |
| **Source query** | Dynamic expression built from metadata parameters |
| **Sink type** | `ParquetSink` → ADLS Gen2 (`ds_adls_parquet_bronze`) |
| **Sink path** | `bronze/{SourceConnectionName}/{TableName}/loaddate={LoadDate}/` |
| **Compression** | Snappy |
| **Retry** | 2 retries, 60 s interval |

**Dynamic source query (INCREMENTAL with hash compare):**
```sql
SELECT hashed.*
FROM (
    SELECT
        {ColumnTypeList},
        '{LoadDate}' AS loaddate,
        HASHBYTES('SHA2_256', CONCAT_WS('|', {ColumnList})) AS row_hash,
        GETDATE() AS bronze_watermark,
        CAST(NULL AS INT) AS IsDeleted
    FROM OPENROWSET(
        BULK '/{SourceConnectionName}/{TableName}/loaddate={LoadDate}/*.parquet',
        DATA_SOURCE = 'landingzone',
        FORMAT = 'PARQUET'
    ) lz
) hashed
LEFT JOIN bronze.vw_{SourceConnectionName}_{TableName} tgt
    ON hashed.[{PrimaryKey}] = tgt.[{PrimaryKey}]
WHERE tgt.[{PrimaryKey}] IS NULL     -- new record
   OR hashed.row_hash <> tgt.row_hash -- changed record
```

**Full load (no watermark / first run):** Same query without the LEFT JOIN / WHERE clause.

### Delete Detection Copy Activity (`act_copy_deletes`)

Triggered only when `DetectDeletes = true` AND a prior watermark exists:

```sql
SELECT tgt.{ColumnList}, tgt.loaddate, tgt.row_hash,
       GETDATE() AS bronze_watermark, 1 AS IsDeleted
FROM bronze.vw_{SourceConnectionName}_{TableName} tgt
LEFT JOIN (
    SELECT [{PrimaryKey}]
    FROM OPENROWSET(
        BULK '/{SourceConnectionName}/{TableName}/loaddate={LoadDate}/*.parquet',
        DATA_SOURCE = 'landingzone', FORMAT = 'PARQUET'
    ) src
) lz ON tgt.[{PrimaryKey}] = lz.[{PrimaryKey}]
WHERE lz.[{PrimaryKey}] IS NULL
  AND tgt.IsDeleted = 0
```

### Silver Copy Activity (`act_copy_bronze_to_silver`)

Default dedup query (overridable via `ctrl.SourceTable.SilverQuery`):

```sql
SELECT * FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY [{PrimaryKey}]
               ORDER BY bronze_watermark DESC
           ) AS _rn
    FROM bronze.vw_{SourceConnectionName}_{TableName}
    WHERE IsDeleted = 0 OR IsDeleted IS NULL
) t WHERE _rn = 1
```

### Gold Copy Activity (`act_copy_silver_to_gold`)

Default pass-through (overridable via `ctrl.SourceTable.GoldQuery`):

```sql
SELECT *
FROM OPENROWSET(
    BULK '/{SourceConnectionName}/{TableName}/*/*.parquet',
    DATA_SOURCE = 'silver', FORMAT = 'PARQUET'
) src
```

Custom example — customer dimension:

```sql
SELECT
    CustomerKey, GivenName + ' ' + Surname AS FullName,
    City, Country, Age, Occupation
FROM OPENROWSET(
    BULK 'contoso/customer/*/*.parquet',
    DATA_SOURCE = 'silver', FORMAT = 'PARQUET'
) src
```

---

## 6. Incremental Load Strategy

| Concept | Implementation |
|---------|---------------|
| **Watermark store** | `ctrl.Watermark` — one row per source table |
| **Initial load** | No watermark row → `WatermarkTS = ''` (empty string, **not NULL** — pipeline variables are typed as String) → full OPENROWSET load, no hash compare |
| **Subsequent loads** | Watermark exists → hash compare LEFT JOIN filters to changed rows only |
| **Watermark update** | `ctrl.usp_upsert_watermark` called at end of every successful Bronze run; sets `LastWatermarkTS = GETDATE()` |
| **Partition-based landing** | Landing zone files are already partitioned by `loaddate=yyyy-MM-dd`; each Bronze run reads only today's partition |
| **Restartability** | Re-running a pipeline for the same `LoadDate` overwrites the same Bronze partition folder; the hash compare guarantees idempotency |
| **Full reload** | Set `LoadType = 'FULL'` in `ctrl.SourceTable` and delete the `ctrl.Watermark` row to force a complete re-ingest |

---

## 7. Error Handling & Logging Design

### Execution Log Flow

```
act_log_start  (INSERT, Status = RUNNING)
      │
      ▼
[Copy Activities / IfCondition]
      │
   ┌──┴──┐
Success  Failure
   │        │
act_log_success   act_log_failure
(Status = SUCCESS) (Status = FAILED, ErrorMessage = ...)
```

### Activity-level error handling

| Activity | On Failure |
|----------|-----------|
| `act_copy_inserts_updates` | Triggers `act_log_failure` via `dependencyConditions: ["Failed"]` |
| `act_if_detect_deletes` | Triggers `act_log_failure` |
| `act_upsert_watermark` | Triggers `act_log_failure` |
| Child pipelines in ForEach | Each child handles its own logging; master ForEach continues other tables |

### Retry policy (all Copy Activities)

```
timeout:              1 hour
retry:                2 retries
retryIntervalInSeconds: 60
```

### Monitoring queries

```sql
-- Recent failures
SELECT PipelineName, TableName, Layer, StartTime, ErrorMessage
FROM ctrl.PipelineExecution
WHERE Status = 'FAILED'
ORDER BY StartTime DESC;

-- Row count audit
SELECT TableName, Layer, LoadDate, RowsWritten
FROM ctrl.PipelineExecution
WHERE Status = 'SUCCESS'
ORDER BY StartTime DESC;

-- Long-running executions
SELECT *, DATEDIFF(MINUTE, StartTime, ISNULL(EndTime, GETDATE())) AS DurationMinutes
FROM ctrl.PipelineExecution
ORDER BY StartTime DESC;
```

---

## 8. End-to-End Execution Flow

```
Trigger (schedule / manual)
        │
        ▼
pl_master_orchestrator
        │
        ▼
act_lookup_active_tables ──► ctrl.SourceTable (IsActive=1)
        │
        ▼
act_foreach_table  (parallelism = 5)
        │
        ├──► [table 1] ──────────────────────────────────────┐
        │                                                      │
        ├──► [table 2] ──► (same flow as table 1 below)       │
        │                                                      │
        └──► [table N]                                         │
                                                               ▼
                                            act_exec_landing_to_bronze
                                                    │
                                       ┌────────────▼────────────┐
                                       │  pl_landing_to_bronze   │
                                       │                         │
                                       │  SetVariable: RunId     │
                                       │  SetVariable: LoadDate  │
                                       │  Lookup: Watermark ──► ctrl.Watermark
                                       │  SetVariable: WatermarkTS
                                       │  Lookup: Log Start ──► ctrl.PipelineExecution (RUNNING)
                                       │                         │
                                       │  Copy: INSERTS/UPDATES  │
                                       │   Source: Serverless SQL│
                                       │   (OPENROWSET + CAST +  │
                                       │    HASHBYTES + LEFT JOIN)│
                                       │   Sink: ADLS Bronze     │
                                       │        Parquet          │
                                       │                         │
                                       │  IfCondition: DetectDeletes?
                                       │   └─ TRUE: Copy DELETES │
                                       │       (IsDeleted=1)     │
                                       │                         │
                                       │  Script: Upsert Watermark ──► ctrl.Watermark
                                       │  Script: Log SUCCESS ──► ctrl.PipelineExecution
                                       └────────────────────────┘
                                                    │
                                            SilverEnabled = 1?
                                                    │ YES
                                       ┌────────────▼────────────┐
                                       │  pl_bronze_to_silver    │
                                       │                         │
                                       │  Copy: DEDUP SELECT     │
                                       │   Source: Serverless SQL│
                                       │   (ROW_NUMBER() dedup)  │
                                       │   Sink: ADLS Silver     │
                                       └────────────────────────┘
                                                    │
                                            GoldEnabled = 1?
                                                    │ YES
                                       ┌────────────▼────────────┐
                                       │  pl_silver_to_gold      │
                                       │                         │
                                       │  Copy: GOLD SELECT      │
                                       │   Source: Serverless SQL│
                                       │   (OPENROWSET silver or │
                                       │    custom GoldQuery)    │
                                       │   Sink: ADLS Gold       │
                                       └────────────────────────┘
```

**Data lake path convention:**

| Zone | Path |
|------|------|
| Landing | `landingzone/{connection}/{table}/loaddate={yyyy-MM-dd}/*.parquet` |
| Bronze | `bronze/{connection}/{table}/loaddate={yyyy-MM-dd}/*.parquet` |
| Silver | `silver/{connection}/{table}/loaddate={yyyy-MM-dd}/*.parquet` |
| Gold | `gold/{connection}/{table}/loaddate={yyyy-MM-dd}/*.parquet` |

**Bronze view convention** (existing pattern, unchanged):

```sql
CREATE OR ALTER VIEW bronze.vw_{connection}_{table} AS
SELECT *, ROW_NUMBER() OVER (PARTITION BY [{PrimaryKey}] ORDER BY bronze_watermark DESC) AS rn
FROM OPENROWSET(BULK '{connection}/{table}/*/*.parquet', DATA_SOURCE='bronze', FORMAT='PARQUET') src
-- used by hash compare and delete detection queries
```

---

## 9. Migration Plan

### Phase 0 — Prerequisites (no data movement)

| Step | Action | Owner |
|------|--------|-------|
| 0.1 | Provision Azure SQL Database (`metadata` db) with `ctrl` schema | DBA |
| 0.2 | Run `ctrl_create_metadata_tables.json` to create tables | DBA |
| 0.3 | Run `ctrl_stored_procedures.json` to create helper procs | DBA |
| 0.4 | Grant Synapse workspace Managed Identity `db_datareader` + `db_datawriter` on `metadata` db | DBA |
| 0.5 | Update `ls_azuresql_metadata.json` server/database values | Engineer |
| 0.6 | Run `Build.json` to confirm external data sources on serverless pool | Engineer |
| 0.7 | Create Bronze views for each source table (see `Customer view.json` as template) **before running the first incremental load** — the hash compare LEFT JOIN requires `bronze.vw_{connection}_{table}` to exist | Engineer |

### Phase 1 — Parallel run (Bronze only)

| Step | Action |
|------|--------|
| 1.1 | Seed `ctrl.SourceTable` with one table (e.g. `contoso/customer`) — seed data included in `ctrl_create_metadata_tables.json` |
| 1.2 | Trigger `pl_landing_to_bronze` manually for the seeded table |
| 1.3 | Verify Bronze parquet output matches existing CETAS output |
| 1.4 | Verify `ctrl.Watermark` and `ctrl.PipelineExecution` rows |
| 1.5 | Compare row counts and hash values between old and new Bronze partitions |

### Phase 2 — Full table onboarding

| Step | Action |
|------|--------|
| 2.1 | Insert remaining source tables into `ctrl.SourceTable` |
| 2.2 | Run `pl_master_orchestrator` in schedule (or on-demand) |
| 2.3 | Enable `SilverEnabled = 1` per table once Bronze is validated |
| 2.4 | Enable `GoldEnabled = 1` per table once Silver is validated |

### Phase 3 — Decommission stored procedure

| Step | Action |
|------|--------|
| 3.1 | Remove scheduled calls to `[Bronze].[usp_load_cetas_partition]` |
| 3.2 | Archive `sp_load_cetas_partition.json` (do not delete — audit trail) |
| 3.3 | Monitor `ctrl.PipelineExecution` for 2 weeks before confirming migration complete |

---

## 10. Risks & Optimisation Opportunities

### Risks

| Risk | Severity | Mitigation |
|------|----------|-----------|
| **Dynamic SQL injection** | Medium | All parameters sourced from `ctrl.SourceTable` (controlled internal table); no user input at runtime |
| **Serverless SQL cold-start latency** | Low-Medium | First query of the day may take 15–30 s to spin up; increase Copy Activity timeout accordingly |
| **Large ColumnTypeList string** | Low | Synapse expression engine limit is 100 KB; monitor for very wide tables |
| **Bronze view missing for first run** | Medium | The hash compare LEFT JOIN references `bronze.vw_*`; ensure view exists before first incremental run (created by `Customer view.json` pattern) |
| **Concurrent writes to same partition** | Low | `pl_master_orchestrator` assigns one table per ForEach item; no two iterations write to the same path |
| **Azure SQL metadata DB single point of failure** | Medium | Use Azure SQL Business Critical tier with zone redundancy for production |

### Optimisation Opportunities

| Opportunity | Description |
|-------------|-------------|
| **Partition pruning** | Landing files are partitioned by `loaddate`; OPENROWSET reads only today's partition — no full scans |
| **Parallel copy within table** | For very large tables, split by partition column using Copy Activity `partitionOption` |
| **Silver/Gold caching** | Store Silver query results as a dedicated Serverless SQL EXTERNAL TABLE for BI tools to query directly |
| **Schema drift detection** | Add a `SchemaVersion` column to `ctrl.SourceTable`; compare actual Parquet schema against registered version using a pre-copy Script Activity |
| **Custom Gold models** | Populate `ctrl.SourceTable.GoldQuery` with dimensional model SQL (star schema, SCD2, aggregations) for BI-ready Gold tables without code changes |
| **Trigger strategy** | Use a tumbling window trigger (daily) for `pl_master_orchestrator` to guarantee exactly-once execution per day per table |
| **Cost control** | Script Activity to call `ctrl.usp_upsert_watermark` is the only non-Copy work; Serverless SQL is billed per TB scanned — keep source partitions tight |

---

## Repository Structure

```
├── dataset/
│   ├── ds_adls_parquet_landing.json     # Landing zone parquet (parameterised)
│   ├── ds_adls_parquet_bronze.json      # Bronze zone parquet (parameterised)
│   ├── ds_adls_parquet_silver.json      # Silver zone parquet (parameterised)
│   ├── ds_adls_parquet_gold.json        # Gold zone parquet (parameterised)
│   ├── ds_azuresql_control.json         # Azure SQL metadata store
│   ├── ds_serverless_sql_source.json    # Serverless SQL Copy source
│   └── Synapse_bronze.json              # (legacy)
├── linkedService/
│   ├── ls_azuresql_metadata.json        # Azure SQL metadata store linked service
│   ├── AzureDataLakeStorage1.json       # ADLS Gen2
│   ├── AzureSynapseAnalytics1.json      # Serverless SQL pool
│   └── ...
├── pipeline/
│   ├── pl_master_orchestrator.json      # Top-level metadata-driven orchestrator
│   ├── pl_landing_to_bronze.json        # Landing → Bronze (hash compare, deletes)
│   ├── pl_bronze_to_silver.json         # Bronze → Silver (dedup, business logic)
│   └── pl_silver_to_gold.json           # Silver → Gold (BI-ready models)
└── sqlscript/
    ├── ctrl_create_metadata_tables.json # DDL: ctrl schema + seed data
    ├── ctrl_stored_procedures.json      # Watermark upsert + execution logging procs
    ├── sp_load_cetas_partition.json     # (legacy – retained for reference)
    ├── Build.json                       # External data sources setup
    ├── Create Table.json                # Bronze external table example
    ├── Customer view.json               # Bronze view example
    └── ...
```
