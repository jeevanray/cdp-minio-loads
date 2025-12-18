Below is an **updated, corrected config example** (kept in the same JSON-style you shared) that shows exactly how to prepare **incremental (daily)** vs **discover (full/historic/backfill)** configs, and how to use **columns selection**.

Key fixes applied:
- JSON booleans must be `true/false` (not `True/False`) if this is truly JSON.[1]
- Every active object must have `delta_column` because the extraction always uses the partition predicate on that column.
- Added `selector_mode` + `lookback_days` defaults and per-object overrides.
- Added `columns` examples (either `"*"` or comma-separated list).

> Convention in these examples:  
> - `load_type` can still be `"delta"` / `"historic"` for backward compatibility in your codebase, but the **actual date selection** is driven by `selector_mode`.  
> - `delta_column_value` is internally the partition watermark (YYYY-MM-DD) in audit.

***

## Updated config (example)

```json
{
  "uds_to_minio": {
    "notification": false,
    "metadata_is_file_based": false,
    "sftp_file_format": "parquet",
    "destination_type": "minio",

    "audit_config": {
      "schema": "appuser",
      "audit_table": "AIRFLOW_CDP_DIAPI_RUN_LOG"
    },

    "schema": "uds",
    "output_path": "segmentation/segmentdb_6550/input_data/customer_data/customer_profile",

    "chunksize": 100000,
    "delta_column_type": "date",

    "selector_mode": "incremental",
    "lookback_days": 1,

    "objects": [
      {
        "isactive": true,
        "db_table": "test_table",
        "schema": "appuser",

        "load_type": "delta",

        "delta_column": "AUD_DT",
        "delta_column_type": "date",

        "ORDER_BY": "",
        "columns": "*",

        "chunksize": 1000000,
        "output_path": "segmentation/segmentdb_6550/input_data/customer_data/test_table",
        "schedule": "daily",

        "selector_mode": "incremental",
        "lookback_days": 1
      },

      {
        "isactive": true,
        "db_table": "customer_audit",
        "schema": "appuser",

        "load_type": "historic",

        "delta_column": "AUD_DATE",
        "delta_column_type": "date",

        "ORDER_BY": "ID",

        "columns": "ID, CIF, AUD_DATE, STATUS, CREATED_AT",

        "chunksize": 100000,
        "output_path": "segmentation/segmentdb_6550/input_data/customer_data/customer_audit",
        "schedule": "historic",

        "selector_mode": "discover"
      },

      {
        "isactive": false,
        "db_table": "customer_profile",
        "schema": "cdp_unica_refined",

        "load_type": "delta",

        "delta_column": "AUD_DT",
        "delta_column_type": "date",

        "ORDER_BY": "",
        "columns": "*",

        "chunksize": 1000000,
        "output_path": "segmentation/segmentdb_6550/input_data/customer_data/customer_profile",
        "schedule": "daily",

        "selector_mode": "incremental",
        "lookback_days": 1
      },

      {
        "isactive": false,
        "db_table": "retail_gdm_cust_dim",
        "schema": "uds",

        "load_type": "historic",

        "delta_column": "AUD_DT",
        "delta_column_type": "date",

        "ORDER_BY": "CIF",
        "columns": "*",

        "output_path": "segmentation/segmentdb_6550/input_data/customer_data/retail_gdm_cust_dim/",
        "chunksize": 1000000,

        "selector_mode": "discover"
      },

      {
        "isactive": false,
        "db_table": "SS_DETAIL_CONVERSION_CAMPAIGN_REPORT_MVW_RAW",
        "schema": "uds",

        "load_type": "delta",

        "delta_column": "AUD_DT",
        "delta_column_type": "date",

        "ORDER_BY": "",
        "columns": "*",

        "chunksize": 1000000,
        "output_path": "segmentation/segmentdb_6550/input_data/customer_data/SS_DETAIL_CONVERSION_CAMPAIGN_REPORT_MVW_RAW/"
      },

      {
        "isactive": false,
        "db_table": "cif_ua_contacthistory",
        "schema": "campaign",

        "load_type": "historic",

        "delta_column": "CONTACTDATETIME",
        "delta_column_type": "timestamp",
        "delta_column_format": "%d-%m-%Y %H:%M:%S",

        "ORDER_BY": "CIF",

        "columns": "*",

        "chunksize": 1000000,
        "output_path": "segmentation/segmentdb_6550/input_data/customer_data/cif_ua_contacthistory/",

        "selector_mode": "discover"
      }
    ]
  }
}
```

***

## How to decide config values

### Incremental (daily) tables
Use:
- `selector_mode: "incremental"`
- `lookback_days: 1` (for T‑1 late data)
- `delta_column: <your partition date column>`
- `ORDER_BY` optional (if absent, pipeline reruns the full partition on failure)

### Discover / Historic (backfill) tables
Use:
- `selector_mode: "discover"`
- `delta_column` must exist
- For timestamp columns, include `delta_column_format` (as you did)
- `ORDER_BY` recommended (CIF/ID) to enable faster mid-partition restarts

### Selecting columns
Set:
- `"columns": "*"` for full table
or
- `"columns": "COL1, COL2, COL3"` for subset

(Your code already builds `SELECT {columns} FROM {table}`.)

***

If you tell the **standard delta_column naming convention** in your org (is it usually `AUD_DT`?), I can suggest a defaulting rule so you don’t have to repeat it in every object.

[1](https://json-schema.org/understanding-json-schema/reference/boolean)
[2](https://www.w3schools.com/js/js_json_datatypes.asp)
[3](https://apidog.com/blog/json-boolean-understanding-and-implementing-boolean-values-in-json/)
[4](https://www.reddit.com/r/learnprogramming/comments/lo8fjw/how_does_json_handle_booleans_and_numbers/)
[5](https://docs.oracle.com/en/database/oracle/oracle-database/26/adjsn/using-sql-json-function-json_value-boolean-json-value.html)
[6](https://gravitino.apache.org/docs/next/trino-connector/catalog-hive/)
[7](https://docs.oracle.com/en/database/oracle/oracle-database/23/adjsn/using-sql-json-function-json_value-boolean-json-value.html)
[8](https://docs.arenadata.io/en/ADH/current/how-to/trino/trino-connector-hive.html)
[9](https://community.safe.com/data-7/why-does-a-json-reader-convert-json-boolean-values-i-e-true-false-to-yes-no-boolean-values-16280)
[10](https://trino.io/docs/current/security/file-system-access-control.html)
[11](https://learn.microsoft.com/en-us/power-platform/power-fx/reference/function-boolean)
[12](https://trino.io/docs/current/connector/iceberg.html)
[13](https://docs.squiz.net/component-service/latest/input-types/input-type-boolean.html)
[14](https://github.com/prestosql/presto/issues/4796)
[15](https://www.youtube.com/watch?v=gw52qPPiykE)
[16](https://trino.io/docs/current/connector/hive.html)
[17](https://trino.io/docs/current/object-storage/metastores.html)
[18](https://datastrato.ai/docs/0.5.1/apache-hive-catalog)
[19](https://docs.starburst.io/latest/connector/hive.html)




------------------------

File **3/3**: `CDP_db2_minio_copy.py` (FULL FILE)

This version is based on your current repo file and applies only the necessary upgrades:

### What is implemented
- **Partition path layout**: always `.../partition_dt=YYYY-MM-DD` (both “delta” and “historic” modes).
- **Partition filter** (Oracle): always uses your preferred range predicate  
  `>= TO_DATE(:partition_dt,'YYYY-MM-DD') AND < TO_DATE(:partition_dt,'YYYY-MM-DD') + 1`
- **Schema mismatch fix**: project `oracle_schema` to `cursor.description` columns before calling `create_arrow_table_from_rows`.
- **Keyset restart** using `order_by` (CIF typically):
  - audit `restart_point` stores **last key value**, not chunk number.
  - If `ORDER_BY` missing: cautious mode → no resume mid-partition; rerun that partition from scratch.
- **New selector plumbing**: passes `selector_mode` and `lookback_days` to `get_load_status_and_dates()` from YAML.
- **Trino hook after each partition completes** (best-effort):
  - Creates external table `hive.default.<MY_TABLE>` (only table part, per your rule)
  - Sync partitions using `CALL hive.system.sync_partition_metadata('default','MY_TABLE','ADD')`[1]
  - Never fails/retries MinIO load if Trino fails.
  - Logs Trino outcome using **Option A** into the same audit row via `aerospike_error` (non-strict marker).

> Note: You said you will integrate Trino connection logic. So this code expects an optional `trino_conn` to be wired in `run_single_object_from_payload()` and passed down. If not provided, the hook logs and skips.

***

```python
import io
import json
import argparse
from typing import Dict, Any, Optional, List
from datetime import datetime
import pytz

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pyarrow as pa
import pyarrow.parquet as pq

from constants import DATETIMEFORMAT
from minio_helper.minio_handler import init_minio_client, upload_table, delete_file
from minio_helper.audit import prepare_auditing, update_audit_record_strict, initialize_restart_audit_log
from minio_helper.utilities import (
    connect_to_oracle,
    close_connection,
    get_oracle_table_schema,
    create_arrow_table_from_rows,
    get_system_config,
    get_job_config,
)
from minio_helper.restartable_logic import get_load_status_and_dates
from minio_helper.logconfig import get_logger

from minio_helper.trino_external import ensure_external_table_and_sync_partitions

logger = get_logger("Minio_framework")
IST = pytz.timezone("Asia/Kolkata")


def generate_object_path(
    base_path: str,
    business_loaddt: str,
    load_type: str,
    delta_column_value: Optional[str] = None,
    sub_folder: Optional[str] = None
) -> str:
    """
    Always write Hive-style partition folders:
      {base_path}/partition_dt=YYYY-MM-DD

    - delta load  -> uses business_loaddt
    - historic    -> uses delta_column_value
    """
    if not base_path:
        raise ValueError("base_path is required")

    if load_type == "historic":
        if not delta_column_value:
            raise ValueError("delta_column_value is required for historic load type")
        dt = datetime.strptime(str(delta_column_value).strip(), "%Y-%m-%d")
        partition_value = dt.strftime("%Y-%m-%d")
    else:
        dt = datetime.strptime(str(business_loaddt).strip(), "%Y-%m-%d")
        partition_value = dt.strftime("%Y-%m-%d")

    parts = [base_path.rstrip("/")]
    if sub_folder:
        parts.append(str(sub_folder).strip("/"))
    parts.append(f"partition_dt={partition_value}")
    return "/".join(parts)


def _root_location_from_effective_path(effective_object_path: str) -> str:
    """
    effective_object_path:
      <base>/partition_dt=YYYY-MM-DD
    returns:
      <base>/
    """
    if "/partition_dt=" in effective_object_path:
        return effective_object_path.split("/partition_dt=")[0].rstrip("/") + "/"
    return effective_object_path.rstrip("/") + "/"


def project_schema_to_query_columns(oracle_schema: pa.Schema, column_names: List[str]) -> pa.Schema:
    """
    Return schema containing only columns returned by the SELECT (cursor.description),
    in the same order as column_names.
    """
    field_by_name = {f.name.upper(): f for f in oracle_schema}
    projected_fields = []
    for c in column_names:
        key = str(c).strip().upper()
        f = field_by_name.get(key)
        if f is not None:
            projected_fields.append(f)
    return pa.schema(projected_fields)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
       retry=retry_if_exception_type(Exception))
def upload_arrow_table_parquet(minio_client, table, bucket_name: str,
                               object_path, compression: str = "snappy") -> None:
    """
    Upload PyArrow Table as parquet (no pandas).
    Uses minio_helper.upload_table first; falls back to put_object.
    """
    try:
        try:
            return upload_table(minio_client, table, bucket_name, object_path, compression=compression)
        except Exception:
            buffer = io.BytesIO()
            pq.write_table(table, buffer, compression=(compression or "snappy"))
            buffer.seek(0)
            if hasattr(minio_client, "put_object"):
                minio_client.put_object(
                    bucket_name,
                    object_path,
                    buffer,
                    buffer.getbuffer().nbytes,
                    content_type="application/octet-stream",
                )
            else:
                raise RuntimeError("Provided minio client does not support put_object")
    except Exception as e:
        logger.error("Failed to upload Arrow table to %s: %s", object_path, e)
        raise


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
       retry=retry_if_exception_type(Exception))
def oracle_to_minio_parquet(
    oracle_config: Dict[str, Any],
    minio_config: Dict[str, Any],
    config_audit: Dict[str, Any],
    table_name: str,
    base_object_path: str,
    business_loaddt: str,
    *,
    chunk_size: int = 100_000,
    compression: str = "snappy",
    order_by: Optional[str] = None,
    restart_point: Optional[str] = None,          # now keyset marker (string), not chunk index
    load_type: str = "delta",
    delta_column: Optional[str] = None,           # used to filter by partition date
    delta_column_value: Optional[str] = None,     # partition_dt (YYYY-MM-DD)
    sub_folder: Optional[str] = None,
    delta_column_type: Optional[str] = None,      # not used for predicate now (kept for compatibility)
    where_clause: Optional[str] = "",
    columns: Optional[str] = "*",
    trino_conn=None,                              # optional best-effort hook
) -> None:
    """
    Extract data from Oracle to MinIO parquet using PyArrow.
    Partition predicate is ALWAYS date-range on delta_column and delta_column_value.
    Restart uses keyset pagination if order_by is provided.
    """
    if not table_name or not table_name.strip():
        raise ValueError("table_name is required and cannot be empty")
    if not base_object_path:
        raise ValueError("base_object_path is required")
    if not minio_config.get("bucket_name"):
        raise ValueError("MinIO bucket_name is required in configuration")

    datetime.strptime(business_loaddt, "%Y-%m-%d")

    # Partition watermark is delta_column_value (YYYY-MM-DD)
    partition_dt = (delta_column_value or business_loaddt)
    datetime.strptime(partition_dt, "%Y-%m-%d")

    if not delta_column:
        # delta_column is required for correct partition filtering
        raise ValueError("delta_column is required (used as partition filter column)")

    extraction_start = datetime.now(IST)
    bucket = minio_config["bucket_name"]

    effective_object_path = generate_object_path(
        base_object_path, business_loaddt, load_type, partition_dt, sub_folder
    ).replace("//", "/")

    audit_log = prepare_auditing()
    audit_log.update({
        "source_table": table_name.upper(),
        "business_loaddt": business_loaddt,
        "delta_column_value": partition_dt,   # watermark selector stored here
        "load_type": load_type,
        "task_startts": extraction_start.strftime(DATETIMEFORMAT),
        "status": "RUNNING",
        "minio_filepath": effective_object_path,
    })

    # Loads existing audit row (restart_point, totals, etc.)
    initialize_restart_audit_log(config_audit, audit_log, business_loaddt, partition_dt)

    # If already completed for this partition, skip
    if audit_log.get("status") == "COMPLETED" and str(audit_log.get("delta_column_value")) == partition_dt:
        logger.info("Job already COMPLETED for %s partition_dt=%s. Skipping.", table_name, partition_dt)
        return

    # Determine restart marker:
    # - If caller passed restart_point -> use it
    # - Else use audit_log.restart_point
    # - Only apply if order_by exists (keyset mode)
    keyset_marker = None
    if order_by:
        keyset_marker = restart_point or audit_log.get("restart_point")

    # If no order_by, be cautious: do not attempt partial resume
    if not order_by and audit_log.get("status") in ("FAILED", "RUNNING"):
        logger.warning(
            "No ORDER_BY provided for %s. Will rerun full partition %s from scratch (no mid-partition restart).",
            table_name, partition_dt
        )
        audit_log["restart_point"] = None

    logger.info(
        "Starting extraction for %s partition_dt=%s load_type=%s order_by=%s restart_key=%s",
        table_name, partition_dt, load_type, order_by, keyset_marker
    )

    # Schema retrieval
    oracle_schema, _arrow_type_map = get_oracle_table_schema(oracle_config, table_name)
    logger.info("Retrieved Oracle schema with %d columns for %s", len(oracle_schema), table_name)

    # Build partition predicate using bind variable (your preferred pattern)
    # WHERE date_col >= TO_DATE(:partition_dt,'YYYY-MM-DD')
    #   AND date_col <  TO_DATE(:partition_dt,'YYYY-MM-DD') + 1
    where_parts = [
        f"{delta_column} >= TO_DATE(:partition_dt, 'YYYY-MM-DD')",
        f"{delta_column} < TO_DATE(:partition_dt, 'YYYY-MM-DD') + 1",
    ]
    binds: Dict[str, Any] = {"partition_dt": partition_dt}

    # optional extra where clause (config-driven)
    if where_clause:
        where_parts.append(f"({where_clause})")

    # keyset restart filter
    if order_by and keyset_marker:
        # bind last key; assumes order_by is a simple column expression
        where_parts.append(f"{order_by} > :last_key")
        binds["last_key"] = keyset_marker

    where_sql = " WHERE " + " AND ".join(where_parts)
    order_clause = f" ORDER BY {order_by}" if order_by else ""
    select_sql = f"SELECT {columns} FROM {table_name}{where_sql}{order_clause}"

    conn = None
    cur = None
    mclient = None
    try:
        conn = connect_to_oracle(oracle_config)
        mclient = init_minio_client(minio_config)

        cur = conn.cursor()
        cur.arraysize = max(10_000, min(chunk_size, 100_000))

        logger.info("Executing SELECT for streaming: %s", select_sql)
        cur.execute(select_sql, binds)

        # Use cursor.description (actual query columns), not schema field list
        column_names = [col[0] for col in cur.description]
        projected_schema = project_schema_to_query_columns(oracle_schema, column_names)

        # Identify order_by column index for keyset tracking (if applicable)
        order_idx = None
        if order_by:
            order_col = order_by.split()[-1].split(".")[-1].strip().upper()
            col_map = {str(c).strip().upper(): i for i, c in enumerate(column_names)}
            if order_col in col_map:
                order_idx = col_map[order_col]
            else:
                logger.warning(
                    "ORDER_BY column %s not found in SELECT list for %s; keyset restart disabled.",
                    order_by, table_name
                )
                order_by = None
                order_idx = None

        chunk_index = 0
        while True:
            rows = cur.fetchmany(chunk_size)
            if not rows:
                logger.info("No more data to process for %s partition_dt=%s", table_name, partition_dt)
                break

            table_chunk = create_arrow_table_from_rows(rows, column_names, projected_schema)

            # object name (keep chunk index only for uniqueness; restart uses keyset marker)
            object_name = (
                f"{effective_object_path}/{table_name.replace('.', '_')}_{chunk_index:06d}.parquet"
            ).replace("//", "/")

            upload_arrow_table_parquet(mclient, table_chunk, bucket, object_name, compression=compression)

            # Update keyset restart marker after successful upload
            last_key_val = None
            if order_idx is not None:
                try:
                    last_key_val = rows[-1][order_idx]
                except Exception:
                    last_key_val = None

            now_ist = datetime.now(IST)
            current_run_recs = table_chunk.num_rows
            prev_total = int(audit_log.get("total_records", 0) or 0)
            cumulative_total = prev_total + current_run_recs

            current_run_time = (now_ist - extraction_start).total_seconds()
            prev_exec_time = float(audit_log.get("task_exec_secs", 0) or 0)
            cumulative_exec_time = prev_exec_time + float(current_run_time)

            audit_log.update({
                "total_records": cumulative_total,
                "status": "RUNNING",
                "task_endts": now_ist.strftime(DATETIMEFORMAT),
                "task_exec_secs": cumulative_exec_time,
                "extraction_time": cumulative_exec_time,
                "minio_filepath": effective_object_path,
                # restart_point now stores last key value (string) if available
                "restart_point": (str(last_key_val) if last_key_val is not None else None),
            })

            # Strict audit update remains strict for MinIO correctness
            try:
                update_audit_record_strict(config_audit, audit_log)
            except Exception as e:
                logger.error(
                    "Audit update failed after uploading object; deleting uploaded object %s/%s",
                    bucket, object_name
                )
                try:
                    delete_file(mclient, bucket, object_name)
                except Exception:
                    if hasattr(mclient, "remove_object"):
                        mclient.remove_object(bucket, object_name)
                raise RuntimeError(f"Audit update failed after upload (rolled back object): {e}")

            if chunk_index % 10 == 0:
                logger.info(
                    "Chunk %s uploaded: %s rows -> %s/%s",
                    chunk_index, current_run_recs, bucket, object_name
                )
            chunk_index += 1

        # Mark completed
        final_ist = datetime.now(IST)
        exec_time = (final_ist - extraction_start).total_seconds()

        audit_log.update({
            "status": "COMPLETED",
            "task_endts": final_ist.strftime(DATETIMEFORMAT),
            "task_exec_secs": exec_time,
            "extraction_time": exec_time,
        })
        update_audit_record_strict(config_audit, audit_log)
        logger.info("Completed Oracle -> MinIO parquet for %s partition_dt=%s", table_name, partition_dt)

        # -------------------------
        # Trino hook (best effort)
        # -------------------------
        try:
            root_path = _root_location_from_effective_path(effective_object_path)
            external_location = f"s3a://{bucket}/{root_path}"

            trino_fqtn = ensure_external_table_and_sync_partitions(
                trino_conn=trino_conn,
                oracle_table_name=table_name.upper(),
                arrow_schema=oracle_schema,
                external_location=external_location,
                catalog="hive",
                schema="default",
                partition_column="partition_dt",
                partition_type="date",
                sync_mode="ADD",
            )

            # Option A logging (non-strict) in same audit row:
            # append small marker; do not fail if this audit update fails
            try:
                prev = audit_log.get("aerospike_error") or ""
                marker = f"TRINO:SUCCESS:{trino_fqtn}"
                audit_log["aerospike_error"] = (prev + ("|" if prev else "") + marker)
                update_audit_record_strict(config_audit, audit_log)
            except Exception:
                pass

            logger.info("Trino external table ensured & partitions synced: %s", trino_fqtn)

        except Exception as te:
            # Never fail MinIO job due to Trino
            logger.error("Trino hook failed (non-blocking) for %s: %s", table_name, te)
            try:
                prev = audit_log.get("aerospike_error") or ""
                marker = f"TRINO:FAILED:{str(te)[:500]}"
                audit_log["aerospike_error"] = (prev + ("|" if prev else "") + marker)
                update_audit_record_strict(config_audit, audit_log)
            except Exception:
                pass

    except Exception as e:
        final_ist = datetime.now(IST)
        audit_log.update({
            "status": "FAILED",
            "task_endts": final_ist.strftime(DATETIMEFORMAT),
            "task_exec_secs": (final_ist - extraction_start).total_seconds(),
            "aerospike_error": str(e),
        })
        try:
            update_audit_record_strict(config_audit, audit_log)
        except Exception:
            pass
        logger.error("Oracle -> MinIO transfer failed: %s", e)
        raise RuntimeError(f"Extraction failed: {e}")
    finally:
        close_connection(cur, conn)


def process_oracle_to_minio_with_dependencies(
    oracle_config: Dict[str, Any],
    minio_config: Dict[str, Any],
    config_audit: Dict[str, Any],
    table_name: str,
    base_object_path: str,
    current_business_loaddt: str,
    *,
    chunk_size: int = 100_000,
    compression: str = "snappy",
    order_by: Optional[str] = None,
    load_type: str = "delta",
    delta_column: Optional[str] = None,
    sub_folder: Optional[str] = None,
    delta_column_type: Optional[str] = None,
    delta_column_format: Optional[str] = None,
    where_clause: Optional[str] = "",
    columns: Optional[str] = "*",
    selector_mode: str = "incremental",
    lookback_days: int = 1,
    trino_conn=None,
) -> None:
    """
    Sequential processing - processes ALL incomplete jobs in order (from selector).
    """
    table_name_uc = table_name.upper()
    logger.info(
        "Begin processing Oracle->MinIO for %s up to %s (load_type=%s selector_mode=%s)",
        table_name_uc, current_business_loaddt, load_type, selector_mode
    )

    dates = get_load_status_and_dates(
        config_audit=config_audit,
        source_table=table_name_uc,
        current_business_loaddt=current_business_loaddt,
        load_type=load_type,
        delta_column=delta_column,
        delta_column_type=delta_column_type,
        delta_column_format=delta_column_format,
        oracle_config=oracle_config,
        selector_mode=selector_mode,
        lookback_days=lookback_days,
    )

    if not dates:
        logger.info("No jobs to process - all completed or no jobs found.")
        return

    for date_info in dates:
        biz_dt = date_info["business_loaddt"]
        status = date_info["status"]
        delta_value = date_info.get("delta_column_value")  # partition_dt
        info_load_type = date_info.get("load_type", load_type)

        # restart_point now can be a key string (CIF) or None
        restart_point = date_info.get("restart_point")

        logger.info(
            "Processing job partition_dt=%s status=%s restart_point=%s load_type=%s",
            delta_value, status, restart_point, info_load_type
        )

        if status == "RUNNING":
            logger.warning("Job is RUNNING; treating as FAILED for restart.")
            status = "FAILED"

        if status in ("NOT_STARTED", "FAILED"):
            oracle_to_minio_parquet(
                oracle_config=oracle_config,
                minio_config=minio_config,
                config_audit=config_audit,
                table_name=table_name_uc,
                base_object_path=base_object_path,
                business_loaddt=biz_dt,
                chunk_size=chunk_size,
                compression=compression,
                order_by=order_by,
                restart_point=(restart_point if status == "FAILED" else None),
                load_type=info_load_type,
                delta_column=delta_column,                # required partition filter column
                delta_column_value=delta_value,           # watermark/partition value (YYYY-MM-DD)
                sub_folder=sub_folder,
                delta_column_type=delta_column_type,
                where_clause=where_clause,
                columns=columns,
                trino_conn=trino_conn,
            )

            logger.info("Successfully completed processing for partition_dt=%s", delta_value)

    logger.info("All required processing completed for %s", table_name_uc)


def run_single_object_from_payload(
    obj_config: dict,
    *,
    config_yaml: str,
    section: str,
    current_business_loaddt: str,
):
    conn_config = get_system_config()
    cfg = get_job_config(config_yaml)[section]

    columns = obj_config.get("columns", "*")

    oracle_config = conn_config.get("target", {})
    minio_config = conn_config.get("minio", {})

    effective_schema = obj_config.get("schema", cfg.get("schema", "appuser"))
    if effective_schema.lower() == "campaign":
        oracle_config = conn_config.get("campaign", {})

    audit_config = {
        "target": conn_config.get("target", {}),
        "schema": "uds",
        "audit_table": cfg.get("audit_config", {}).get("audit_table", "AIRFLOW_CDP_DIAPI_RUN_LOG"),
    }

    load_type = obj_config.get("load_type", "delta")
    where_clause = obj_config.get("where_clause", "")

    table_name = f"{effective_schema}.{obj_config['db_table']}"
    output_path = obj_config.get("output_path", cfg.get("output_path"))
    if not output_path:
        raise ValueError(f"output_path not found for object {obj_config}")

    chunk_size = obj_config.get("chunksize", cfg.get("chunksize", 100_000))
    order_by = obj_config.get("ORDER_BY")

    # Partition date column in Oracle (required now)
    delta_column = obj_config.get("delta_column") or cfg.get("delta_column")
    if not delta_column:
        raise ValueError("delta_column is required in config (used for partition filter predicate)")

    delta_column_type = (obj_config.get("delta_column_type", cfg.get("delta_column_type", "date")) or "date").lower()
    delta_column_format = obj_config.get("delta_column_format", cfg.get("delta_column_format", ""))

    # selector settings
    selector_mode = (obj_config.get("selector_mode") or cfg.get("selector_mode") or "incremental").lower()
    lookback_days = int(obj_config.get("lookback_days") or cfg.get("lookback_days") or 1)

    # historic-specific legacy fields (kept for compatibility, not used in path anymore)
    sub_folder = obj_config.get("sub_folder") if load_type == "historic" else None

    # ---- Trino connection wiring placeholder ----
    # User will integrate this part:
    trino_conn = None

    process_oracle_to_minio_with_dependencies(
        oracle_config=oracle_config,
        minio_config=minio_config,
        config_audit=audit_config,
        table_name=table_name,
        base_object_path=output_path,
        current_business_loaddt=current_business_loaddt,
        chunk_size=chunk_size,
        compression="snappy",
        order_by=order_by,
        load_type=load_type,
        delta_column=delta_column,
        sub_folder=sub_folder,
        delta_column_type=("timestamp" if delta_column_type == "timestamp" else "date"),
        delta_column_format=delta_column_format,
        where_clause=where_clause,
        columns=columns,
        selector_mode=selector_mode,
        lookback_days=lookback_days,
        trino_conn=trino_conn,
    )


def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--config-yaml", required=True)
    p.add_argument("--section", required=True)
    p.add_argument("--object", required=True)
    p.add_argument("--business-date", default=None)
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    obj = json.loads(args.object)
    biz_date = args.business_date or datetime.now(IST).strftime("%Y-%m-%d")

    run_single_object_from_payload(
        obj_config=obj,
        config_yaml=args.config_yaml,
        section=args.section,
        current_business_loaddt=biz_date,
    )
```

***

## Important notes / assumptions

1) **delta_column is now mandatory** for both incremental and discover modes because the Oracle query always filters by partition date using that column (your required predicate).  
2) `restart_point` is now stored as a string key (CIF) in audit. Any other code that casts it to int must be removed (we already fixed that in `restartable_logic.py`).  
3) Trino hook runs after MinIO COMPLETED and is best-effort. Table creation + partition sync uses Trino Hive connector procedure.[1]

***

## Before you run: confirm these two config items exist per object

- `delta_column: <your date/timestamp column>`  
- optionally:
  - `selector_mode: incremental|discover`
  - `lookback_days: 1`

If you want, paste one sample YAML object block and I’ll validate it matches the new expectations.

[1](https://trino.io/docs/current/connector/hive.html)


-------------------------------------------------------------

File **2/3**: `minio_helper/trino_external.py` (FULL FILE)

This file is **connection-agnostic**: it only builds/executes SQL using a provided `trino_conn` (DB-API cursor style), and it is **best-effort friendly** (caller decides whether errors should fail the job). It also enforces your naming rule:

- `APPUSER.MY_TABLE` → `hive.default.MY_TABLE`

It uses `system.sync_partition_metadata(..., 'ADD')` which is the supported Trino Hive connector procedure to register filesystem partitions into the metastore.[1]

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pyarrow as pa
from minio_helper.logconfig import get_logger

logger = get_logger("TRINO_EXTERNAL")


@dataclass(frozen=True)
class TrinoTableRef:
    catalog: str = "hive"
    schema: str = "default"
    table: str = ""


def oracle_table_to_trino_table_name(oracle_table: str) -> str:
    """
    Oracle: APPUSER.MY_TABLE -> Trino table: MY_TABLE

    Assumption confirmed by user:
    - schema is always hive.default from config
    - table name should match Oracle table name part (after dot)
    """
    if not oracle_table or not oracle_table.strip():
        raise ValueError("oracle_table is required")
    return oracle_table.split(".")[-1].strip().upper()


def _arrow_to_trino_type(dt: pa.DataType) -> str:
    """
    Minimal safe mapping for external Parquet tables.
    Extend if your Oracle types include more complex mappings.
    """
    if pa.types.is_string(dt) or pa.types.is_large_string(dt):
        return "varchar"
    if pa.types.is_binary(dt) or pa.types.is_large_binary(dt):
        return "varbinary"
    if pa.types.is_boolean(dt):
        return "boolean"

    if pa.types.is_int8(dt) or pa.types.is_int16(dt) or pa.types.is_int32(dt):
        return "integer"
    if pa.types.is_int64(dt):
        return "bigint"

    if pa.types.is_float32(dt):
        return "real"
    if pa.types.is_float64(dt):
        return "double"

    if pa.types.is_decimal(dt):
        return f"decimal({dt.precision},{dt.scale})"

    if pa.types.is_date32(dt) or pa.types.is_date64(dt):
        return "date"

    if pa.types.is_timestamp(dt):
        # Keeping it simple; Trino supports timestamp type.
        return "timestamp"

    # fallback (safer than failing table creation)
    return "varchar"


def build_create_external_table_sql(
    *,
    table_ref: TrinoTableRef,
    arrow_schema: pa.Schema,
    external_location: str,
    partition_column: str = "partition_dt",
    partition_type: str = "date",
) -> str:
    """
    Create Hive external table over Parquet files, partitioned by partition_column.

    Important: partition column must be the LAST column in table definition for Trino/Hive.
    [web:119]
    """
    if not table_ref.table:
        raise ValueError("table_ref.table is required")
    if not external_location:
        raise ValueError("external_location is required")

    cols = []
    for f in arrow_schema:
        cols.append(f'"{f.name}" {_arrow_to_trino_type(f.type)}')

    # Must be last
    cols.append(f'"{partition_column}" {partition_type}')

    cols_sql = ",\n  ".join(cols)

    return f"""
CREATE TABLE IF NOT EXISTS {table_ref.catalog}.{table_ref.schema}."{table_ref.table}" (
  {cols_sql}
)
WITH (
  external_location = '{external_location}',
  format = 'PARQUET',
  partitioned_by = ARRAY['{partition_column}']
)
""".strip()


def build_sync_partitions_sql(*, table_ref: TrinoTableRef, mode: str = "ADD") -> str:
    """
    mode:
      - ADD: add partitions present on filesystem but missing in metastore [web:118]
      - DROP: drop partitions missing on filesystem
      - FULL: some distros support FULL; keep to ADD for safety
    """
    mode = (mode or "ADD").upper()
    if mode not in ("ADD", "DROP", "FULL"):
        raise ValueError("mode must be one of ADD/DROP/FULL")

    return (
        f"CALL {table_ref.catalog}.system.sync_partition_metadata"
        f"('{table_ref.schema}', '{table_ref.table}', '{mode}')"
    )


def ensure_external_table_and_sync_partitions(
    *,
    trino_conn,
    oracle_table_name: str,
    arrow_schema: pa.Schema,
    external_location: str,
    catalog: str = "hive",
    schema: str = "default",
    partition_column: str = "partition_dt",
    partition_type: str = "date",
    sync_mode: str = "ADD",
) -> str:
    """
    Creates table if not exists and sync partitions.
    Returns the fully-qualified Trino table name.
    """
    trino_table = oracle_table_to_trino_table_name(oracle_table_name)
    table_ref = TrinoTableRef(catalog=catalog, schema=schema, table=trino_table)

    ddl = build_create_external_table_sql(
        table_ref=table_ref,
        arrow_schema=arrow_schema,
        external_location=external_location,
        partition_column=partition_column,
        partition_type=partition_type,
    )
    sync_sql = build_sync_partitions_sql(table_ref=table_ref, mode=sync_mode)

    cur = trino_conn.cursor()

    logger.info("Creating external table if not exists: %s.%s.%s", catalog, schema, trino_table)
    cur.execute(ddl)

    logger.info("Syncing partitions (%s) for: %s.%s.%s", sync_mode, catalog, schema, trino_table)
    cur.execute(sync_sql)

    return f"{catalog}.{schema}.{trino_table}"
```

**Notes**
- Uses `sync_partition_metadata(..., 'ADD')`, which adds filesystem partitions not yet present in metastore.[1]
- Ensures partition column is last (important for correct partitioned table behavior).[2]
- Does not swallow exceptions internally; **caller** should catch and treat it non-blocking (as we do in `CDP_db2_minio_copy.py`).

***

Reply **next** and I will output **File 3/3: `CDP_db2_minio_copy.py` (FULL FILE)** updated with:
- `partition_dt=YYYY-MM-DD` layout
- schema projection fix
- keyset restart using last `order_by` value (CIF)
- new selector_mode / lookback_days plumbing into `get_load_status_and_dates`
- Trino hook after partition completion (best-effort) + Option A audit logging (non-strict)

[1](https://trino.io/docs/current/connector/hive.html)
[2](https://stackoverflow.com/questions/74124390/how-to-create-a-partitioned-trino-table-on-s3-with-sub-folders)
[3](https://trino.io/episodes/5.html)
[4](https://stackoverflow.com/questions/74311288/trino-hive-connector-cant-synchronize-the-partition-metadata-automatically)
[5](https://github.com/trinodb/trino/issues/20071)
[6](https://github.com/trinodb/trino/issues/22756)
[7](https://stackoverflow.com/questions/43346332/create-hive-external-table-from-partitioned-parquet-files-in-azure-hdinsights)
[8](https://docs.starburst.io/latest/connector/hive.html)
[9](https://www.starburst.io/community/forum/t/hive-external-table-with-projection-partition/249)
[10](https://techblog.lycorp.co.jp/en/migration-from-hive-to-trino-and-spark)
[11](https://www.starburst.io/community/forum/t/hive-external-table-with-projection-partition/249/)
[12](https://www.youtube.com/watch?v=OXyJFZSsX5w)
[13](https://trino.io/docs/current/release/release-377.html)
[14](https://community.cloudera.com/t5/Support-Questions/Create-Hive-table-to-read-parquet-files-from-parquet-avro/td-p/98544)
[15](https://stackoverflow.com/questions/65944123/how-read-data-partitons-in-s3-from-trino)
[16](https://trino.io/docs/current/connector/iceberg.html)
[17](https://github.com/apache/hudi/issues/4437)
[18](https://docs.starburst.io/starburst-galaxy/data-engineering/working-with-data-lakes/table-formats/gl-hive.html)
[19](https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html)
[20](https://github.com/trinodb/trino/discussions/19042)

----------------------------------------------------
Starting **File 1/3**: `minio_helper/restartable_logic.py` (FULL FILE)

This version adds:

- `selector_mode="incremental"` (audit-watermark with lookback for T‑1)
- `selector_mode="discover"` (distinct discovery cached parquet + subtract completed)
- Uses **delta_column_value** as the watermark/partition value
- Removes any `int(restart_point)` assumptions (restart_point can now be last CIF/keyset marker)
- Keeps behavior compatible with your current pipeline entrypoints and signatures

Late arriving (T‑1) support is via a configurable lookback window, which is a standard approach.[1]

```python
import os
import logging
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timedelta, date as dt_date

import pandas as pd
import oracledb
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from minio_helper.utilities import connect_to_oracle
from constants import ORACLE_DATE

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


PARQUET_BASEPATH = "/opt/airflow/etl_inward_files/CDP_minio/"


def _norm_date_str(val: Any) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    # Expect already YYYY-MM-DD or something parseable by pandas; normalize to YYYY-MM-DD
    try:
        return pd.to_datetime(s).strftime("%Y-%m-%d")
    except Exception:
        return s


def _parquet_cache_path(source_table: str) -> str:
    file_name = f"source_delta_{source_table.replace('.', '_')}.parquet"
    os.makedirs(PARQUET_BASEPATH, exist_ok=True)
    return os.path.join(PARQUET_BASEPATH, file_name)


def _refresh_distinct_dates_to_parquet(
    parquet_file: str,
    oracle_config: Dict[str, Any],
    source_table: str,
    delta_column: str,
    delta_column_type: Optional[str],
    delta_column_format: Optional[str],
) -> None:
    query = (
        f"SELECT /*+ PARALLEL(4) */ DISTINCT {delta_column} AS DELTA_VALUE "
        f"FROM {source_table} WHERE {delta_column} IS NOT NULL "
        f"ORDER BY {delta_column}"
    )
    with connect_to_oracle(oracle_config) as conn:
        df = pd.read_sql(query, conn)

    if "DELTA_VALUE" not in df.columns:
        raise RuntimeError("Distinct delta query did not return DELTA_VALUE column")

    # Normalize to YYYY-MM-DD strings
    if delta_column_type:
        t = delta_column_type.lower()
        if t in ("timestamp", "date"):
            df["DELTA_VALUE"] = pd.to_datetime(
                df["DELTA_VALUE"],
                format=(delta_column_format or None),
                errors="coerce",
            ).dt.strftime("%Y-%m-%d")

    df = df.dropna(subset=["DELTA_VALUE"]).drop_duplicates()
    df.to_parquet(parquet_file, index=False)
    logger.info("Saved refreshed distinct delta values to %s.", parquet_file)


def _load_distinct_dates(
    oracle_config: Dict[str, Any],
    source_table: str,
    delta_column: str,
    delta_column_type: Optional[str],
    delta_column_format: Optional[str],
) -> List[str]:
    parquet_file = _parquet_cache_path(source_table)

    if not os.path.exists(parquet_file):
        logger.info("Parquet cache %s not found. Refreshing distinct delta values.", parquet_file)
        _refresh_distinct_dates_to_parquet(
            parquet_file=parquet_file,
            oracle_config=oracle_config,
            source_table=source_table,
            delta_column=delta_column,
            delta_column_type=delta_column_type,
            delta_column_format=delta_column_format,
        )
    else:
        logger.debug("Using cached parquet file %s", parquet_file)

    df = pd.read_parquet(parquet_file)

    if "DELTA_VALUE" not in df.columns:
        # Backward compatibility: some earlier writes used lowercase/alias mismatch
        possible = [c for c in df.columns if c.strip().lower() in ("delta_value", "delta")]
        if possible:
            df = df.rename(columns={possible[0]: "DELTA_VALUE"})
        else:
            raise RuntimeError(f"Parquet cache {parquet_file} does not contain DELTA_VALUE column")

    deltas = (
        df["DELTA_VALUE"]
        .astype(str)
        .map(_norm_date_str)
        .dropna()
        .drop_duplicates()
        .tolist()
    )
    deltas.sort()
    return deltas


def _get_completed_delta_values(config_audit: Dict[str, Any], source_table: str) -> Set[str]:
    q = f"""
        SELECT DISTINCT delta_column_value
        FROM {config_audit["schema"]}.{config_audit["audit_table"]}
        WHERE source_table = :src
          AND status = 'COMPLETED'
          AND delta_column_value IS NOT NULL
    """
    with connect_to_oracle(config_audit["target"]) as conn:
        with conn.cursor() as cur:
            cur.execute(q, {"src": source_table})
            out: Set[str] = set()
            for (v,) in cur.fetchall():
                s = _norm_date_str(v)
                if s:
                    out.add(s)
            return out


def _get_last_audit_row(config_audit: Dict[str, Any], source_table: str) -> Optional[Dict[str, Any]]:
    q = f"""
        SELECT
            business_loaddt,
            NVL(status, 'NOT_STARTED') AS status,
            restart_point,
            NVL(total_records, 0) AS total_records,
            delta_column_value,
            NVL(load_type, 'delta') AS load_type,
            NVL(task_exec_secs, 0) AS task_exec_secs,
            NVL(extraction_time, 0) AS extraction_time,
            aerospike_error
        FROM {config_audit["schema"]}.{config_audit["audit_table"]}
        WHERE source_table = :src
          AND delta_column_value IS NOT NULL
        ORDER BY updated_at_ts DESC NULLS LAST
        FETCH FIRST 1 ROW ONLY
    """
    with connect_to_oracle(config_audit["target"]) as conn:
        with conn.cursor() as cur:
            cur.execute(q, {"src": source_table})
            row = cur.fetchone()
            if not row:
                return None

            business_dt, status, restart_point, total_records, delta_val, load_type, task_exec_secs, extraction_time, aerospike_error = row
            if isinstance(aerospike_error, oracledb.LOB):
                aerospike_error = aerospike_error.read()

            biz_str = (
                business_dt.strftime("%Y-%m-%d")
                if hasattr(business_dt, "strftime")
                else str(business_dt)
            ) if business_dt else None

            return {
                "business_loaddt": biz_str,
                "status": status,
                "restart_point": (str(restart_point) if restart_point is not None else None),
                "total_records": int(total_records or 0),
                "delta_column_value": _norm_date_str(delta_val),
                "load_type": load_type,
                "task_exec_secs": float(task_exec_secs or 0),
                "extraction_time": float(extraction_time or 0),
                "aerospike_error": aerospike_error,
            }


def _max_completed_delta_value(config_audit: Dict[str, Any], source_table: str) -> Optional[str]:
    q = f"""
        SELECT MAX(delta_column_value)
        FROM {config_audit["schema"]}.{config_audit["audit_table"]}
        WHERE source_table = :src
          AND status = 'COMPLETED'
          AND delta_column_value IS NOT NULL
    """
    with connect_to_oracle(config_audit["target"]) as conn:
        with conn.cursor() as cur:
            cur.execute(q, {"src": source_table})
            row = cur.fetchone()
            return _norm_date_str(row[0]) if row and row[0] else None


def _date_range(start: dt_date, end: dt_date) -> List[str]:
    out = []
    d = start
    while d <= end:
        out.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return out


def _jobs_from_dates(
    dates: List[str],
    base_audit: Optional[Dict[str, Any]] = None,
    load_type: str = "delta",
) -> List[Dict[str, Any]]:
    base_audit = base_audit or {}
    jobs: List[Dict[str, Any]] = []
    for ds in dates:
        jobs.append({
            "business_loaddt": ds,                 # kept for compatibility with existing code
            "status": "NOT_STARTED",
            "restart_point": None,                # will be keyset marker (CIF etc) or None
            "total_records": int(base_audit.get("total_records", 0) or 0),
            "task_exec_secs": float(base_audit.get("task_exec_secs", 0) or 0),
            "extraction_time": float(base_audit.get("extraction_time", 0) or 0),
            "delta_column_value": ds,             # watermark selector = partition_dt
            "load_type": load_type,
        })
    return jobs


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
       retry=retry_if_exception_type(Exception))
def get_load_status_and_dates(
    config_audit: Dict[str, Any],
    source_table: str,
    current_business_loaddt: str,
    load_type: str = "delta",
    delta_column: Optional[str] = None,
    delta_column_type: Optional[str] = None,
    delta_column_format: Optional[str] = None,
    oracle_config: Optional[Dict[str, Any]] = None,
    selector_mode: str = "incremental",
    lookback_days: int = 1,
) -> List[Dict[str, Any]]:
    """
    Returns list of partition-date jobs to run.

    selector_mode:
      - "incremental": watermark from audit + lookback (T-1 support)
      - "discover": distinct dates from Oracle (cached parquet) - completed

    watermark field: delta_column_value (YYYY-MM-DD).
    """
    if not current_business_loaddt:
        raise ValueError("current_business_loaddt is required")

    end_dt = datetime.strptime(current_business_loaddt, "%Y-%m-%d").date()
    selector_mode = (selector_mode or "incremental").lower()

    completed = _get_completed_delta_values(config_audit, source_table)
    last_row = _get_last_audit_row(config_audit, source_table) or {}

    if selector_mode == "discover":
        if not (oracle_config and delta_column):
            raise ValueError("oracle_config and delta_column are required for selector_mode='discover'")

        all_deltas = _load_distinct_dates(
            oracle_config=oracle_config,
            source_table=source_table,
            delta_column=delta_column,
            delta_column_type=delta_column_type,
            delta_column_format=delta_column_format,
        )
        # Only consider <= current_business_loaddt
        all_deltas = [d for d in all_deltas if d <= current_business_loaddt]

        pending = sorted(set(all_deltas) - completed)
        if not pending:
            logger.info("No pending partitions (discover) for %s", source_table)
            return []

        logger.info("Discover selector: %d pending partitions for %s", len(pending), source_table)
        return _jobs_from_dates(pending, base_audit=last_row, load_type=load_type)

    # incremental selector (default)
    max_done = _max_completed_delta_value(config_audit, source_table)
    if max_done:
        start_dt = datetime.strptime(max_done, "%Y-%m-%d").date() - timedelta(days=int(lookback_days or 0))
    else:
        start_dt = end_dt - timedelta(days=int(lookback_days or 0))

    # clamp
    if start_dt > end_dt:
        start_dt = end_dt

    candidates = _date_range(start_dt, end_dt)

    # IMPORTANT: current behavior = "missing-only".
    # If you want "reprocess-lookback", remove this filter.
    pending = [d for d in candidates if d not in completed]

    if not pending:
        logger.info("No pending partitions (incremental) for %s", source_table)
        return []

    logger.info(
        "Incremental selector: start=%s end=%s lookback_days=%s pending=%d for %s",
        start_dt.strftime("%Y-%m-%d"),
        end_dt.strftime("%Y-%m-%d"),
        lookback_days,
        len(pending),
        source_table,
    )
    return _jobs_from_dates(pending, base_audit=last_row, load_type=load_type)
```

***

### What changed vs your current `restartable_logic.py`
- “historic” vs “delta” separation is replaced by a **selector_mode** while keeping `load_type` parameter for your existing pipeline compatibility.  
- No more `int(restart_point)` anywhere (restart_point can now be CIF/last key).  
- Discover mode uses *set difference* (all_deltas − completed) instead of “after last completed” so it can handle missing/late partitions.  
- Incremental mode uses audit MAX watermark and a lookback window to cover T‑1 scenarios.[1]

***

Reply “next” and I will generate **File 2/3: `minio_helper/trino_external.py`** (full file) and then **File 3/3: `CDP_db2_minio_copy.py`** (full file, updated with partition path, projected schema, keyset restart, and Trino hook best-effort).

[1](https://lakefs.io/blog/best-practices-late-arriving-data/)


-----------------------------

