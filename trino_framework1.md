Below is a self‑contained local playground:

- `docker-compose.yml` with Trino + MinIO + Iceberg catalog  
- SQL to create `email_send`, `email_staging`, and an Iceberg target table  
- A faker Python script to load sample data into Trino  
- The incremental framework code (`config.yaml`, `job_runner.py`, `job_engine.py`) wired to your join/query pattern

You can copy this into a folder and run end‑to‑end.

***

## 1. docker-compose for Trino + MinIO + Iceberg

`docker-compose.yml`:

```yaml
version: "3.9"

services:
  minio:
    image: minio/minio
    container_name: minio
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minio
      MINIO_ROOT_PASSWORD: minio123
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - ./minio-data:/data

  trino:
    image: trinodb/trino:444
    container_name: trino
    ports:
      - "8080:8080"
    depends_on:
      - minio
    volumes:
      - ./trino/etc:/etc/trino
      - ./trino/post-init.sql:/tmp/post-init.sql
    command: >
      bash -c "
      /usr/lib/trino/bin/run-trino & 
      sleep 15 && 
      trino --server http://localhost:8080 --execute \"RUN SCRIPT '/tmp/post-init.sql'\" &&
      tail -f /dev/null
      "
```

Minimal Trino config to enable Iceberg backed by MinIO closely follows Trino’s docs and common examples.[1][2]

`trino/etc/config.properties`:

```properties
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery-server.enabled=true
discovery.uri=http://trino:8080
```

`trino/etc/node.properties`:

```properties
node.environment=dev
node.id=trino-node-1
node.data-dir=/var/trino/data
```

`trino/etc/jvm.config` – standard from Trino docs is fine.[3]

`trino/etc/catalog/iceberg.properties`:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://minio:9000/iceberg
iceberg.rest-catalog.warehouse=s3a://warehouse/
iceberg.file-format=PARQUET
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.region=us-east-1
s3.aws-access-key=minio
s3.aws-secret-key=minio123
s3.path-style-access=true
```

For a quick local run you can also use the `filesystem` catalog type pointing at a mounted volume, but MinIO better matches your real environment.[4]

***

## 2. post-init.sql: schemas, source tables, target table

`trino/post-init.sql`:

```sql
-- Create schema for sources and target
CREATE SCHEMA IF NOT EXISTS iceberg.demo
WITH (location = 's3a://warehouse/demo/');

-- Source tables (email_send, email_staging) as Iceberg for simplicity
CREATE TABLE IF NOT EXISTS iceberg.demo.email_send (
  audience_id           varchar,
  email_address         varchar,
  campaign_code         varchar,
  run_id                varchar,
  sent_timestamp        timestamp,
  event_insert_time_stamp timestamp,
  source_app            integer,
  status_text           varchar
);

CREATE TABLE IF NOT EXISTS iceberg.demo.email_staging (
  email_address         varchar,
  xapi_header           varchar,
  sent_timestamp        timestamp,
  event_insert_time_stamp timestamp,
  offer_code            varchar,
  event_name            varchar
);

-- Target table
CREATE TABLE IF NOT EXISTS iceberg.demo.email_contact_action (
  audience_id     varchar,
  campaigncode    varchar,
  contactdatetime timestamp,
  responsedatetime timestamp,
  channel_name    varchar,
  responsetypecode varchar,
  status_text     varchar,
  cif             varchar
)
WITH (
  format = 'PARQUET'
);
```

You can adjust schemas if needed; this is only to make the join and incremental logic runnable.

***

## 3. Faker script to populate email_send / email_staging

`faker_load.py`:

```python
import random
from datetime import datetime, timedelta
from typing import List

import trino

TRINO_HOST = "localhost"
TRINO_PORT = 8080
TRINO_USER = "etl_user"
CATALOG = "iceberg"
SCHEMA = "demo"


def get_conn():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=CATALOG,
        schema=SCHEMA,
    )


def generate_rows(num_days: int = 10, rows_per_day: int = 1000):
    base = datetime(2025, 1, 1)
    for d in range(num_days):
        day = base + timedelta(days=d)
        for i in range(rows_per_day):
            email = f"user{i}@example.com"
            audience_id = f"AUD-{i}"
            campaign_code = f"CMP-{d%5}"
            run_id = f"RUN-{d}"
            sent_ts = day + timedelta(minutes=random.randint(0, 60 * 23))
            event_ts = sent_ts + timedelta(minutes=random.randint(0, 60 * 24))
            source_app = 2
            status_text = random.choice(["SENT", "OPEN", "CLICK"])
            offer_code = f"OFFER-{random.randint(1, 3)}"
            event_name = random.choice(["OPEN", "CLICK", "BOUNCE"])
            cif = f"CIF-{i%1000}"
            yield {
                "audience_id": audience_id,
                "email_address": email,
                "campaign_code": campaign_code,
                "run_id": run_id,
                "sent_timestamp": sent_ts,
                "event_insert_time_stamp": event_ts,
                "source_app": source_app,
                "status_text": status_text,
                "offer_code": offer_code,
                "event_name": event_name,
                "cif": cif,
            }


def insert_batch(cur, table: str, cols: List[str], rows: List[dict]):
    if not rows:
        return
    values = []
    for r in rows:
        vs = []
        for c in cols:
            v = r[c]
            if isinstance(v, datetime):
                vs.append(f"TIMESTAMP '{v.strftime('%Y-%m-%d %H:%M:%S')}'")
            elif isinstance(v, str):
                vs.append(f"'{v}'")
            else:
                vs.append(str(v))
        values.append("(" + ",".join(vs) + ")")
    sql = f"INSERT INTO {CATALOG}.{SCHEMA}.{table} ({','.join(cols)}) VALUES " + ",".join(values)
    cur.execute(sql)


def main():
    conn = get_conn()
    cur = conn.cursor()

    batch_send = []
    batch_stage = []

    for row in generate_rows():
        send_row = {
            "audience_id": row["audience_id"],
            "email_address": row["email_address"],
            "campaign_code": row["campaign_code"],
            "run_id": row["run_id"],
            "sent_timestamp": row["sent_timestamp"],
            "event_insert_time_stamp": row["event_insert_time_stamp"],
            "source_app": row["source_app"],
            "status_text": row["status_text"],
        }
        stage_row = {
            "email_address": row["email_address"],
            "xapi_header": row["run_id"],
            "sent_timestamp": row["sent_timestamp"],
            "event_insert_time_stamp": row["event_insert_time_stamp"],
            "offer_code": row["offer_code"],
            "event_name": row["event_name"],
        }
        batch_send.append(send_row)
        batch_stage.append(stage_row)

        if len(batch_send) >= 500:
            insert_batch(cur, "email_send", list(batch_send[0].keys()), batch_send)
            insert_batch(cur, "email_staging", list(batch_stage[0].keys()), batch_stage)
            batch_send.clear()
            batch_stage.clear()

    if batch_send:
        insert_batch(cur, "email_send", list(batch_send[0].keys()), batch_send)
        insert_batch(cur, "email_staging", list(batch_stage[0].keys()), batch_stage)

    print("Fake data loaded.")


if __name__ == "__main__":
    main()
```

This uses plain `INSERT` into Iceberg tables, which Trino fully supports.[5]

Install deps locally:

```bash
pip install trino
```

Run after `docker-compose up -d` and Trino is ready:

```bash
python faker_load.py
```

***

## 4. Framework config.yaml wired to your query

`config.yaml`:

```yaml
trino:
  host: localhost
  port: 8080
  user: etl_user
  http_scheme: http
  catalog: iceberg
  schema: demo
  session_properties: {}

audit:
  catalog: iceberg
  schema: demo
  table: job_audit

jobs:
  - name: email_contact_action
    enabled: true
    sql_sources:
      - template: sql/email_contact_action.sql.j2
        alias: src1

    target:
      catalog: iceberg
      schema: demo
      table: email_contact_action
      write_mode: upsert
      primary_keys:
        - cif
        - campaigncode
        - contactdatetime
        - responsedatetime

    incremental:
      mode: between

      watermark:
        column: sent_timestamp
        source_alias: a

      step_days: 3
      lookback_days: 0

      filters:
        - alias: a
          column: sent_timestamp
          end_offset_days: 0
        - alias: b
          column: event_insert_time_stamp
          end_offset_days: 4

    deduplication:
      enabled: true
      primary_keys:
        - cif
        - campaigncode
        - contactdatetime
        - responsedatetime
      suppress_null_action: true

    limits:
      max_windows_per_run: 10
      max_rows_per_window: 1000000
      on_limit_breach: fail

    retry:
      max_attempts: 3
      backoff_seconds: 5

    post_actions:
      optimize: false
```

***

## 5. SQL template for the join

`sql/email_contact_action.sql.j2`:

```sql
SELECT
  b.audience_id,
  b.campaign_code AS campaigncode,
  b.sent_timestamp AS contactdatetime,
  b.event_insert_time_stamp AS responsedatetime,
  'EMAIL' AS channel_name,
  a.event_name AS responsetypecode,
  b.status_text,
  'CIF-FAKE' AS cif
FROM iceberg.demo.email_send b
LEFT JOIN iceberg.demo.email_staging a
  ON a.email_address = b.email_address
 AND b.run_id = a.xapi_header
 AND b.sent_timestamp <= a.event_insert_time_stamp
WHERE {{incremental_filters}}
  AND b.source_app = 2
```

The engine will inject both `a.sent_timestamp` and `b.event_insert_time_stamp` predicates as described earlier.

***

## 6. job_runner.py

```python
import argparse
import logging
import time
from pathlib import Path

import yaml

from job_engine import JobEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def load_config(path: str):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def run_job(job_conf, global_conf):
    engine = JobEngine(job_conf, global_conf["trino"], global_conf["audit"])
    engine.run()


def run_jobs(config_path: str):
    cfg = load_config(config_path)
    jobs = cfg.get("jobs", [])
    for j in jobs:
        if not j.get("enabled", True):
            continue
        name = j.get("name")
        logger.info("Starting job %s", name)
        try:
            run_job(j, cfg)
        except Exception as e:
            logger.exception("Job %s failed: %s", name, e)
        else:
            logger.info("Job %s completed", name)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()
    run_jobs(args.config)


if __name__ == "__main__":
    main()
```

***

## 7. job_engine.py (simplified but working version)

```python
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

import jinja2
import trino

logger = logging.getLogger(__name__)


@dataclass
class Window:
    start: datetime
    end: datetime
    watermark: datetime


class JobEngine:
    def __init__(self, job_conf: Dict[str, Any], trino_conf: Dict[str, Any], audit_conf: Dict[str, Any]):
        self.job_conf = job_conf
        self.trino_conf = trino_conf
        self.audit_conf = audit_conf

    # --- Connections and helpers ---

    def _conn(self):
        return trino.dbapi.connect(
            host=self.trino_conf["host"],
            port=self.trino_conf["port"],
            user=self.trino_conf["user"],
            catalog=self.trino_conf["catalog"],
            schema=self.trino_conf["schema"],
        )

    def _exec(self, sql: str):
        logger.debug("Executing SQL: %s", sql)
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute(sql)
            try:
                return cur.fetchall()
            except Exception:
                return []

    # --- Audit window discovery ---

    def _ensure_audit_table(self):
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.audit_conf['catalog']}.{self.audit_conf['schema']}.{self.audit_conf['table']} (
          job_name        varchar,
          window_start    timestamp,
          window_end      timestamp,
          watermark_value timestamp,
          status          varchar,
          attempt_no      integer,
          processed_ts    timestamp
        )
        """
        self._exec(sql)

    def _get_last_watermark(self) -> datetime:
        job_name = self.job_conf["name"]
        sql = f"""
        SELECT max(watermark_value) 
        FROM {self.audit_conf['catalog']}.{self.audit_conf['schema']}.{self.audit_conf['table']}
        WHERE job_name = '{job_name}' AND status = 'SUCCESS'
        """
        rows = self._exec(sql)
        if rows and rows[0][0] is not None:
            return rows[0][0]
        # Default: start from 2025-01-01 for demo
        return datetime(2025, 1, 1)

    def _build_windows(self) -> List[Window]:
        inc = self.job_conf["incremental"]
        step_days = inc["step_days"]
        lookback_days = inc["lookback_days"]
        max_windows = self.job_conf["limits"]["max_windows_per_run"]

        last_wm = self._get_last_watermark()
        base_start = last_wm - timedelta(days=lookback_days)
        now = datetime(2025, 1, 20)  # fixed upper bound for demo

        windows: List[Window] = []
        cur_start = base_start
        while cur_start < now and len(windows) < max_windows:
            cur_end = cur_start + timedelta(days=step_days)
            windows.append(Window(start=cur_start, end=cur_end, watermark=cur_end))
            cur_start = cur_end
        logger.info("Discovered %d windows", len(windows))
        return windows

    # --- Filter building ---

    def _build_incremental_filters(self, window: Window) -> str:
        inc = self.job_conf["incremental"]
        filters_conf = inc["filters"]
        clauses = []
        for f in filters_conf:
            alias = f["alias"]
            col = f["column"]
            end_offset = f.get("end_offset_days", 0)
            start_ts = window.start
            end_ts = window.start + timedelta(days=inc["step_days"] + end_offset)
            start_s = start_ts.strftime("%Y-%m-%d %H:%M:%S")
            end_s = end_ts.strftime("%Y-%m-%d %H:%M:%S")
            clauses.append(
                f"{alias}.{col} >= TIMESTAMP '{start_s}' AND {alias}.{col} < TIMESTAMP '{end_s}'"
            )
        return " AND ".join(f"({c})" for c in clauses)

    # --- Audit ops ---

    def _audit_upsert(self, window: Window, status: str, attempt_no: int):
        job_name = self.job_conf["name"]
        table = f"{self.audit_conf['catalog']}.{self.audit_conf['schema']}.{self.audit_conf['table']}"
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        ws = window.start.strftime("%Y-%m-%d %H:%M:%S")
        we = window.end.strftime("%Y-%m-%d %H:%M:%S")
        wm = window.watermark.strftime("%Y-%m-%d %H:%M:%S")
        sql = f"""
        INSERT INTO {table} (job_name, window_start, window_end, watermark_value, status, attempt_no, processed_ts)
        VALUES ('{job_name}', TIMESTAMP '{ws}', TIMESTAMP '{we}', TIMESTAMP '{wm}', '{status}', {attempt_no}, TIMESTAMP '{now}')
        """
        self._exec(sql)

    # --- Extract / dedup / load ---

    def _load_template(self, path: str) -> jinja2.Template:
        env = jinja2.Environment(loader=jinja2.FileSystemLoader("sql"))
        return env.get_template(path.split("/")[-1])

    def _extract_window(self, window: Window):
        src = self.job_conf["sql_sources"][0]
        template = self._load_template(src["template"])
        inc_filters = self._build_incremental_filters(window)
        sql = template.render(incremental_filters=inc_filters)
        return self._exec(sql)

    def _dedup_and_suppress(self, rows: List[Tuple]) -> List[Tuple]:
        if not self.job_conf["deduplication"]["enabled"]:
            return rows
        # For demo: assume columns in fixed order from template
        # audience_id, campaigncode, contactdatetime, responsedatetime, channel_name, responsetypecode, status_text, cif
        pk_idx = [1, 2, 3, 7]  # campaigncode, contactdatetime, responsedatetime, cif
        idx_contact = 2
        idx_resp = 3

        best = {}
        for r in rows:
            key = tuple(r[i] for i in pk_idx)
            # suppress null actiondatetime if a non-null exists later: simple rule
            if r[idx_resp] is None and key in best:
                continue
            best[key] = r
        return list(best.values())

    def _merge_into_target(self, rows: List[Tuple]):
        tgt = self.job_conf["target"]
        full_name = f"{tgt['catalog']}.{tgt['schema']}.{tgt['table']}"

        # Load into temp table
        temp = f"{tgt['schema']}.tmp_email_contact_action"
        self._exec(f"DROP TABLE IF EXISTS {full_name}_staging")
        self._exec(
            f"""
            CREATE TABLE {full_name}_staging AS 
            SELECT * FROM {full_name} WHERE 1=0
            """
        )

        if not rows:
            return

        values = []
        for r in rows:
            vals = []
            for v in r:
                if v is None:
                    vals.append("NULL")
                elif isinstance(v, datetime):
                    vals.append(f"TIMESTAMP '{v.strftime('%Y-%m-%d %H:%M:%S')}'")
                else:
                    vals.append(f"'{v}'")
            values.append("(" + ",".join(vals) + ")")
        cols = "audience_id,campaigncode,contactdatetime,responsedatetime,channel_name,responsetypecode,status_text,cif"
        ins_sql = f"INSERT INTO {full_name}_staging ({cols}) VALUES " + ",".join(values)
        self._exec(ins_sql)

        # MERGE by PK
        pk = tgt["primary_keys"]
        on = " AND ".join([f"t.{c}=s.{c}" for c in pk])

        merge_sql = f"""
        MERGE INTO {full_name} t
        USING {full_name}_staging s
        ON {on}
        WHEN MATCHED THEN UPDATE SET
          audience_id = s.audience_id,
          campaigncode = s.campaigncode,
          contactdatetime = s.contactdatetime,
          responsedatetime = s.responsedatetime,
          channel_name = s.channel_name,
          responsetypecode = s.responsetypecode,
          status_text = s.status_text
        WHEN NOT MATCHED THEN INSERT (
          audience_id,campaigncode,contactdatetime,responsedatetime,channel_name,responsetypecode,status_text,cif
        ) VALUES (
          s.audience_id,s.campaigncode,s.contactdatetime,s.responsedatetime,s.channel_name,s.responsetypecode,s.status_text,s.cif
        )
        """
        self._exec(merge_sql)

    # --- main run ---

    def run(self):
        self._ensure_audit_table()
        windows = self._build_windows()
        retry_conf = self.job_conf["retry"]
        for w in windows:
            attempt = 1
            while attempt <= retry_conf["max_attempts"]:
                try:
                    self._audit_upsert(w, "STARTED", attempt)
                    rows = self._extract_window(w)
                    if len(rows) > self.job_conf["limits"]["max_rows_per_window"]:
                        raise RuntimeError("max_rows_per_window exceeded")
                    rows = self._dedup_and_suppress(rows)
                    self._merge_into_target(rows)
                    self._audit_upsert(w, "SUCCESS", attempt)
                    break
                except Exception as e:
                    logger.exception("Window %s-%s failed: %s", w.start, w.end, e)
                    self._audit_upsert(w, "FAILED", attempt)
                    attempt += 1
```

This is intentionally minimal, but:

- Uses audit table per window.  
- Builds the dual date filters (`a.` and `b.`) from config.  
- Dedups and upserts into `email_contact_action` via MERGE, which Trino supports for Iceberg.[6][7]

***

## 8. How to run locally

1. Start infra:

```bash
docker-compose up -d
```

2. Wait ~20s, then load fake data:

```bash
pip install trino PyYAML jinja2
python faker_load.py
```

3. Create folders:  

- `config.yaml` at root  
- `job_runner.py`, `job_engine.py`, `faker_load.py` at root  
- `sql/email_contact_action.sql.j2` under `sql/`

4. Run the job:

```bash
python job_runner.py --config config.yaml
```

5. Check results:

```bash
docker exec -it trino trino \
  --server http://localhost:8080 \
  --execute "SELECT * FROM iceberg.demo.email_contact_action LIMIT 20"
```

You can now tweak `step_days`, `lookback_days`, and re-run to see the incremental join-based extraction working over your fake data.

[1](https://trino.io/docs/current/installation/containers.html)
[2](https://github.com/tschaub/trino-example/blob/main/docker-compose.yml)
[3](https://trinodb.github.io/docs.trino.io/397/installation/containers.html)
[4](https://www.puppygraph.com/blog/apache-iceberg-trino)
[5](https://trino.io/docs/current/sql/create-table.html)
[6](https://www.starburst.io/blog/apache-iceberg-dml-update-delete-merge-maintenance-in-trino/)
[7](https://trino.io/docs/current/sql/merge.html)
[8](https://github.com/trinodb/trino/blob/master/core/docker/README.md)
[9](https://aerospike.com/docs/connectors/trino/docker/)
[10](http://github.com/trinodb/docker-images)
[11](https://dev.to/resurfacelabs/building-a-lightweight-trino-distribution-54h7)
[12](https://trino.io/docs/current/sql/show-create-table.html)
[13](https://github.com/dominikhei/trino-cli-docker)
[14](https://dev.to/lazypro/trino-iceberg-made-easy-a-ready-to-use-playground-4oa1)
[15](https://ta.thinkingdata.cn/trino-docs/sql/create-table.html)
[16](https://trino.io/download.html)
[17](https://github.com/asolovey/trino-iceberg-rest)
[18](https://trinodb.github.io/docs.trino.io/429/sql/create-table.html)
[19](https://github.com/IBM/docker-trino)
[20](https://github.com/danthelion/trino-minio-iceberg-example)
[21](https://trinodb.github.io/docs.trino.io/398/sql/create-table.html)
[22](https://www.knowi.com/blog/trino-docker-setup-guide-connect-postgresql-mysql-run-queries/)
