Yes—using local disk as a spill area is the right way to make this **memory-bounded** while still doing column projection, because the job can stream MinIO objects to a temp file and then iterate Parquet batches instead of `resp.read()` into RAM.[1]

Below is a rewritten, production-style script that:
- Scans `base_path/history/` **recursively** (entire base_path history tree) and groups by `YYYYMMDD`.  
- Writes to `base_path/partitioned_dt=YYYY-MM-DD/`. (Hive-style `key=value` partition folder naming is standard.)[2]
- Uses local disk for both download and output staging and deletes temp files after each object / output is finalized.  
- Does **not** delete source objects.  
- Projects only selected columns and errors if any are missing.  
- Supports `--skip-if-exists` idempotency via `stat_object`.[3]
- Optional compaction into ~50MB files using Parquet row-group metadata size estimates.[4]
- Writes incrementally with `ParquetWriter` (no full-day in-memory dataframe).[5]

## Script (streaming via local disk)

```python
#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures as cf
import logging
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from minio import Minio
from minio.error import S3Error

import pyarrow as pa
import pyarrow.parquet as pq


HARDCODED_COLUMNS: List[str] = ["CIF", "AUD_DT", "AMOUNT"]
DATE_RE = re.compile(r"^(?P<yyyymmdd>\d{8})$")


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool
    bucket: str


@dataclass(frozen=True)
class JobConfig:
    base_path: str
    columns: List[str]

    tmp_dir: str
    batch_rows: int

    enable_compaction: bool
    target_file_mb: int
    compression: str
    row_group_size: int

    skip_if_exists: bool
    max_workers: int


def norm_prefix(p: str) -> str:
    return p.strip("/")


def join_key(*parts: str) -> str:
    return "/".join([norm_prefix(str(p)) for p in parts if p is not None and str(p).strip("/") != ""])


def yyyymmdd_to_yyyy_mm_dd(yyyymmdd: str) -> str:
    if not DATE_RE.match(yyyymmdd):
        raise ValueError(f"Invalid date folder name (expected YYYYMMDD): {yyyymmdd}")
    return datetime.strptime(yyyymmdd, "%Y%m%d").strftime("%Y-%m-%d")


def object_exists(client: Minio, bucket: str, object_name: str) -> bool:
    try:
        client.stat_object(bucket, object_name)
        return True
    except Exception:
        return False


def iter_history_parquet_objects(client: Minio, bucket: str, base_path: str) -> Iterable[Tuple[str, str]]:
    """
    Scans all parquet objects under:
      {base_path}/history/**

    Only accepts objects matching:
      {base_path}/history/YYYYMMDD/*.parquet
    """
    history_prefix = join_key(base_path, "history") + "/"
    for obj in client.list_objects(bucket, prefix=history_prefix, recursive=True):
        name = obj.object_name
        if not name.lower().endswith(".parquet"):
            continue

        rel = name[len(history_prefix):]  # YYYYMMDD/filename.parquet (expected)
        parts = rel.split("/", 1)
        if len(parts) != 2:
            continue
        yyyymmdd = parts[0]
        if not DATE_RE.match(yyyymmdd):
            continue
        yield yyyymmdd, name


def download_to_file(client: Minio, bucket: str, object_name: str, dst_path: str) -> None:
    """
    Stream object to local file (no full read into memory).
    """
    resp = client.get_object(bucket, object_name)
    try:
        with open(dst_path, "wb") as f:
            for chunk in resp.stream(1024 * 1024):
                f.write(chunk)
    finally:
        resp.close()
        resp.release_conn()


def upload_file(client: Minio, bucket: str, object_name: str, file_path: str) -> None:
    client.fput_object(bucket, object_name, file_path, content_type="application/vnd.apache.parquet")


def validate_required_columns(pf: pq.ParquetFile, required_cols: List[str]) -> None:
    schema = pf.schema_arrow
    missing = [c for c in required_cols if schema.get_field_index(c) == -1]
    if missing:
        raise ValueError(f"Missing required columns in parquet: {missing}")


def estimate_row_group_bytes(pf: pq.ParquetFile, rg_idx: int) -> int:
    # row_group(i).total_byte_size comes from parquet metadata statistics [web:69]
    return int(pf.metadata.row_group(rg_idx).total_byte_size)


def rewrite_single_object_to_writer(
    pf: pq.ParquetFile,
    columns: List[str],
    writer: pq.ParquetWriter,
    batch_rows: int,
    row_group_size: int,
) -> int:
    """
    Writes projected columns from pf into writer, in batches.
    Returns rows written.
    """
    total_rows = 0
    # iter_batches reads streaming batches from a Parquet file [web:86]
    for batch in pf.iter_batches(columns=columns, batch_size=batch_rows):
        tbl = pa.Table.from_batches([batch])
        writer.write_table(tbl, row_group_size=row_group_size)
        total_rows += tbl.num_rows
    return total_rows


def process_day(
    client: Minio,
    mcfg: MinioConfig,
    jcfg: JobConfig,
    yyyymmdd: str,
    object_names: List[str],
) -> Tuple[str, int]:
    yyyy_mm_dd = yyyymmdd_to_yyyy_mm_dd(yyyymmdd)
    out_prefix = join_key(jcfg.base_path, f"partitioned_dt={yyyy_mm_dd}")
    object_names = sorted(object_names)

    target_bytes = int(jcfg.target_file_mb) * 1024 * 1024
    out_idx = 0
    outputs_written = 0

    def dst_name_for(idx: int) -> str:
        return join_key(out_prefix, f"part-{idx:05d}.parquet") if jcfg.enable_compaction else ""

    with tempfile.TemporaryDirectory(prefix="parquet-rewrite-", dir=jcfg.tmp_dir) as tdir:
        tdir_path = Path(tdir)

        if not jcfg.enable_compaction:
            # One-to-one rewrite, streaming download + streaming batches to local output
            for src_obj in object_names:
                base = os.path.basename(src_obj)
                dst_obj = join_key(out_prefix, base)

                if jcfg.skip_if_exists and object_exists(client, mcfg.bucket, dst_obj):
                    logging.info("Skip exists: %s", dst_obj)
                    continue

                src_local = str(tdir_path / f"src-{base}")
                dst_local = str(tdir_path / f"dst-{base}")

                download_to_file(client, mcfg.bucket, src_obj, src_local)
                pf = pq.ParquetFile(src_local)
                validate_required_columns(pf, jcfg.columns)

                schema = pf.schema_arrow
                out_schema = schema.select(jcfg.columns)

                writer = pq.ParquetWriter(dst_local, out_schema, compression=jcfg.compression, use_dictionary=True)  # [web:10]
                try:
                    rewrite_single_object_to_writer(
                        pf=pf,
                        columns=jcfg.columns,
                        writer=writer,
                        batch_rows=jcfg.batch_rows,
                        row_group_size=jcfg.row_group_size,
                    )
                finally:
                    writer.close()

                upload_file(client, mcfg.bucket, dst_obj, dst_local)
                outputs_written += 1

                # delete per-object temp files early
                try:
                    os.remove(src_local)
                except FileNotFoundError:
                    pass
                try:
                    os.remove(dst_local)
                except FileNotFoundError:
                    pass

            return yyyymmdd, outputs_written

        # Compaction mode: pack many input files into ~target_mb outputs
        current_writer: Optional[pq.ParquetWriter] = None
        current_out_local: Optional[str] = None
        current_schema: Optional[pa.Schema] = None
        bytes_in_current = 0

        def open_new_writer() -> None:
            nonlocal current_writer, current_out_local, out_idx, current_schema, bytes_in_current
            while jcfg.skip_if_exists and object_exists(client, mcfg.bucket, dst_name_for(out_idx)):
                logging.info("Skip exists (index bump): %s", dst_name_for(out_idx))
                out_idx += 1
            current_out_local = str(tdir_path / f"out-{out_idx:05d}.parquet")
            current_writer = pq.ParquetWriter(
                current_out_local,
                current_schema,  # must already be set
                compression=jcfg.compression,
                use_dictionary=True,
            )
            bytes_in_current = 0

        def flush_writer() -> None:
            nonlocal current_writer, current_out_local, out_idx, outputs_written, bytes_in_current
            if current_writer is None or current_out_local is None:
                return
            current_writer.close()
            dst_obj = dst_name_for(out_idx)
            if not (jcfg.skip_if_exists and object_exists(client, mcfg.bucket, dst_obj)):
                upload_file(client, mcfg.bucket, dst_obj, current_out_local)
                outputs_written += 1
                logging.info("Wrote compacted: %s approx_bytes=%d", dst_obj, bytes_in_current)
            else:
                logging.info("Skip exists (flush): %s", dst_obj)

            # delete local output file immediately
            try:
                os.remove(current_out_local)
            except FileNotFoundError:
                pass

            current_writer = None
            current_out_local = None
            out_idx += 1
            bytes_in_current = 0

        for src_obj in object_names:
            base = os.path.basename(src_obj)
            src_local = str(tdir_path / f"src-{base}")
            download_to_file(client, mcfg.bucket, src_obj, src_local)

            pf = pq.ParquetFile(src_local)
            validate_required_columns(pf, jcfg.columns)

            if current_schema is None:
                current_schema = pf.schema_arrow.select(jcfg.columns)
                open_new_writer()

            # Write data batch-wise, but size accounting using row-group metadata [web:69]
            for rg in range(pf.num_row_groups):
                rg_tbl = pf.read_row_group(rg, columns=jcfg.columns)
                current_writer.write_table(rg_tbl, row_group_size=jcfg.row_group_size)  # [web:10]
                bytes_in_current += estimate_row_group_bytes(pf, rg)

                if bytes_in_current >= target_bytes:
                    flush_writer()
                    if current_schema is not None:
                        open_new_writer()

            # delete local input immediately
            try:
                os.remove(src_local)
            except FileNotFoundError:
                pass

        flush_writer()
        return yyyymmdd, outputs_written


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True, help="MinIO endpoint host:port")
    ap.add_argument("--access-key", required=True)
    ap.add_argument("--secret-key", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--secure", default="false", choices=["true", "false"])

    ap.add_argument("--base-path", required=True, help="Prefix in bucket, e.g. base_path")
    ap.add_argument("--tmp-dir", default="/tmp", help="Local temp directory base")
    ap.add_argument("--batch-rows", type=int, default=200_000, help="Arrow batch size (rows) per iter_batches")

    ap.add_argument("--enable-compaction", action="store_true")
    ap.add_argument("--target-mb", type=int, default=50)
    ap.add_argument("--compression", default="snappy", choices=["snappy", "zstd", "gzip", "brotli"])
    ap.add_argument("--row-group-size", type=int, default=1_048_576)

    ap.add_argument("--skip-if-exists", action="store_true")
    ap.add_argument("--max-workers", type=int, default=1)

    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    mcfg = MinioConfig(
        endpoint=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
        secure=(args.secure.lower() == "true"),
        bucket=args.bucket,
    )
    jcfg = JobConfig(
        base_path=norm_prefix(args.base_path),
        columns=HARDCODED_COLUMNS,
        tmp_dir=args.tmp_dir,
        batch_rows=int(args.batch_rows),
        enable_compaction=bool(args.enable_compaction),
        target_file_mb=int(args.target_mb),
        compression=str(args.compression),
        row_group_size=int(args.row_group_size),
        skip_if_exists=bool(args.skip_if_exists),
        max_workers=int(args.max_workers),
    )

    client = Minio(
        mcfg.endpoint,
        access_key=mcfg.access_key,
        secret_key=mcfg.secret_key,
        secure=mcfg.secure,
    )

    by_day: Dict[str, List[str]] = {}
    for yyyymmdd, obj_name in iter_history_parquet_objects(client, mcfg.bucket, jcfg.base_path):
        by_day.setdefault(yyyymmdd, []).append(obj_name)

    days = sorted(by_day.keys())
    logging.info("Discovered %d day folders under %s/history/", len(days), jcfg.base_path)

    if jcfg.max_workers <= 1:
        for d in days:
            day, out_cnt = process_day(client, mcfg, jcfg, d, by_day[d])
            logging.info("Done day=%s outputs=%d", day, out_cnt)
    else:
        with cf.ThreadPoolExecutor(max_workers=jcfg.max_workers) as ex:
            futs = [ex.submit(process_day, client, mcfg, jcfg, d, by_day[d]) for d in days]
            for f in cf.as_completed(futs):
                day, out_cnt = f.result()
                logging.info("Done day=%s outputs=%d", day, out_cnt)


if __name__ == "__main__":
    try:
        main()
    except S3Error as e:
        raise SystemExit(f"MinIO error: {e}") from e
```

## Memory behavior (what this guarantees)
- It **does not** load “all Parquet data” into RAM; it streams MinIO objects to disk and reads Parquet in **batches** using `iter_batches`.[1]
- It still uses memory for each batch (that’s unavoidable), but peak RAM is roughly proportional to `--batch-rows` and the width of selected columns, not the full file/day.[1]

If you share typical per-file sizes (and whether some files are multi-GB), the defaults for `--batch-rows` and `--row-group-size` can be tuned to avoid RAM spikes while keeping throughput acceptable.

[1](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html)
[2](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.partitioning.html)
[3](https://python.hotexamples.com/examples/minio/Minio/stat_object/python-minio-stat_object-method-examples.html)
[4](https://www.mungingdata.com/pyarrow/parquet-metadata-min-max-statistics/)
[5](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetWriter.html)



--------------------------


Below is an updated script that (1) **copies** from `base_path/history/YYYYMMDD/` to `base_path/partitioned_dt=YYYY-MM-DD/` (no deletes), (2) projects only selected columns and **fails** if any are missing, (3) optionally compacts small files targeting ~**50 MB** output files using Parquet metadata size estimates, and (4) adds a **skip-if-destination-exists** mode for idempotency/restartability using `stat_object`.[1][2]

## Updated script

```python
#!/usr/bin/env python3
"""
MinIO Parquet rewriter:
  src:  {base_path}/history/YYYYMMDD/*.parquet
  dest: {base_path}/partitioned_dt=YYYY-MM-DD/*.parquet

Features:
- Column projection (size reduction) with fail-fast on missing columns.
- Copy semantics (writes new objects; never deletes source objects).
- Optional compaction of small files into ~target_mb output Parquet files (approx).
- Optional idempotency: skip writing if destination object already exists.

Notes:
- Uses pyarrow Parquet metadata to estimate row group sizes (total_byte_size) [web:69].
- Uses minio.stat_object to detect existing objects [web:67].
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import io
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

from minio import Minio
from minio.error import S3Error

import pyarrow as pa
import pyarrow.parquet as pq


# ---------------------------
# Config defaults
# ---------------------------

HARDCODED_COLUMNS: List[str] = [
    # TODO: replace with config-driven list
    "CIF",
    "AUD_DT",
    "AMOUNT",
]

DATE_RE = re.compile(r"^(?P<yyyymmdd>\d{8})$")


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool
    bucket: str


@dataclass(frozen=True)
class JobConfig:
    base_path: str
    columns: List[str]

    enable_compaction: bool
    target_file_mb: int
    row_group_size: int
    compression: str

    skip_if_exists: bool

    max_workers: int


# ---------------------------
# Helpers
# ---------------------------

def yyyymmdd_to_yyyy_mm_dd(yyyymmdd: str) -> str:
    if not DATE_RE.match(yyyymmdd):
        raise ValueError(f"Invalid date folder name (expected YYYYMMDD): {yyyymmdd}")
    dt = datetime.strptime(yyyymmdd, "%Y%m%d")
    return dt.strftime("%Y-%m-%d")


def norm_prefix(p: str) -> str:
    return p.strip("/")


def join_key(*parts: str) -> str:
    return "/".join([norm_prefix(str(p)) for p in parts if p is not None and str(p).strip("/") != ""])


def object_exists(client: Minio, bucket: str, object_name: str) -> bool:
    # stat_object succeeds if object exists, otherwise raises [web:67]
    try:
        client.stat_object(bucket, object_name)
        return True
    except Exception:
        return False


def get_object_bytes(client: Minio, bucket: str, object_name: str) -> bytes:
    resp = client.get_object(bucket, object_name)
    try:
        return resp.read()
    finally:
        resp.close()
        resp.release_conn()


def put_object_bytes(client: Minio, bucket: str, object_name: str, data: bytes) -> None:
    bio = io.BytesIO(data)
    client.put_object(bucket, object_name, bio, length=len(data), content_type="application/vnd.apache.parquet")


def iter_history_parquet_objects(
    client: Minio, bucket: str, base_path: str
) -> Iterable[Tuple[str, str]]:
    """
    Yields (yyyymmdd, object_name) for objects under:
      {base_path}/history/YYYYMMDD/*.parquet
    """
    history_prefix = join_key(base_path, "history") + "/"
    for obj in client.list_objects(bucket, prefix=history_prefix, recursive=True):
        name = obj.object_name
        if not name.lower().endswith(".parquet"):
            continue

        rel = name[len(history_prefix):]  # YYYYMMDD/file.parquet
        parts = rel.split("/", 1)
        if len(parts) != 2:
            continue

        yyyymmdd = parts[0]
        if not DATE_RE.match(yyyymmdd):
            continue

        yield yyyymmdd, name


def validate_required_columns(pf: pq.ParquetFile, required_cols: List[str]) -> None:
    schema = pf.schema_arrow
    missing = [c for c in required_cols if schema.get_field_index(c) == -1]
    if missing:
        raise ValueError(f"Missing required columns in parquet: {missing}")


def estimate_row_group_bytes(pf: pq.ParquetFile, rg_idx: int) -> int:
    # row_group(i).total_byte_size is exposed in metadata [web:69]
    md = pf.metadata
    return int(md.row_group(rg_idx).total_byte_size)


# ---------------------------
# Rewrite implementations
# ---------------------------

def rewrite_day_one_to_one(
    client: Minio,
    mcfg: MinioConfig,
    jcfg: JobConfig,
    yyyymmdd: str,
    object_names: List[str],
) -> int:
    """
    One input parquet -> one output parquet under partitioned_dt=YYYY-MM-DD/.
    """
    yyyy_mm_dd = yyyymmdd_to_yyyy_mm_dd(yyyymmdd)
    out_prefix = join_key(jcfg.base_path, f"partitioned_dt={yyyy_mm_dd}")

    written = 0
    for src_obj in sorted(object_names):
        base = os.path.basename(src_obj)
        dst_obj = join_key(out_prefix, base)

        if jcfg.skip_if_exists and object_exists(client, mcfg.bucket, dst_obj):
            logging.info("Skip exists: %s", dst_obj)
            continue

        raw = get_object_bytes(client, mcfg.bucket, src_obj)
        pf = pq.ParquetFile(pa.BufferReader(raw))
        validate_required_columns(pf, jcfg.columns)

        out_buf = io.BytesIO()
        writer: Optional[pq.ParquetWriter] = None
        try:
            for rg in range(pf.num_row_groups):
                tbl = pf.read_row_group(rg, columns=jcfg.columns)
                if writer is None:
                    writer = pq.ParquetWriter(
                        out_buf,
                        tbl.schema,
                        compression=jcfg.compression,
                        use_dictionary=True,
                    )
                writer.write_table(tbl, row_group_size=jcfg.row_group_size)
        finally:
            if writer is not None:
                writer.close()

        put_object_bytes(client, mcfg.bucket, dst_obj, out_buf.getvalue())
        written += 1

    return written


def rewrite_day_compacted_target_mb(
    client: Minio,
    mcfg: MinioConfig,
    jcfg: JobConfig,
    yyyymmdd: str,
    object_names: List[str],
) -> int:
    """
    Compacts by packing row groups into output parquet files until estimated bytes >= target.
    Estimation uses input row group metadata total_byte_size (compressed-ish but not exact) [web:69].
    """
    yyyy_mm_dd = yyyymmdd_to_yyyy_mm_dd(yyyymmdd)
    out_prefix = join_key(jcfg.base_path, f"partitioned_dt={yyyy_mm_dd}")
    target_bytes = int(jcfg.target_file_mb) * 1024 * 1024

    out_idx = 0
    out_written = 0

    writer: Optional[pq.ParquetWriter] = None
    writer_buf: Optional[io.BytesIO] = None
    out_schema: Optional[pa.Schema] = None
    bytes_in_current = 0

    def next_dst_object(idx: int) -> str:
        return join_key(out_prefix, f"part-{idx:05d}.parquet")

    def flush() -> None:
        nonlocal writer, writer_buf, out_idx, out_written, bytes_in_current
        if writer is None or writer_buf is None:
            return

        writer.close()
        dst_obj = next_dst_object(out_idx)

        if jcfg.skip_if_exists and object_exists(client, mcfg.bucket, dst_obj):
            logging.info("Skip exists (compacted): %s", dst_obj)
        else:
            put_object_bytes(client, mcfg.bucket, dst_obj, writer_buf.getvalue())
            out_written += 1
            logging.info("Wrote compacted: %s approx_bytes=%d", dst_obj, bytes_in_current)

        out_idx += 1
        writer = None
        writer_buf = None
        bytes_in_current = 0

        # If destination exists and skip is on, keep incrementing index until a free name is found
        # so we don't spin forever.
        if jcfg.skip_if_exists:
            while object_exists(client, mcfg.bucket, next_dst_object(out_idx)):
                logging.info("Skip exists (index bump): %s", next_dst_object(out_idx))
                out_idx += 1

    try:
        # If skip is enabled, start at first free index
        if jcfg.skip_if_exists:
            while object_exists(client, mcfg.bucket, next_dst_object(out_idx)):
                logging.info("Skip exists (start bump): %s", next_dst_object(out_idx))
                out_idx += 1

        for src_obj in sorted(object_names):
            raw = get_object_bytes(client, mcfg.bucket, src_obj)
            pf = pq.ParquetFile(pa.BufferReader(raw))
            validate_required_columns(pf, jcfg.columns)

            for rg in range(pf.num_row_groups):
                tbl = pf.read_row_group(rg, columns=jcfg.columns)
                if out_schema is None:
                    out_schema = tbl.schema

                if writer is None:
                    writer_buf = io.BytesIO()
                    writer = pq.ParquetWriter(
                        writer_buf,
                        out_schema,
                        compression=jcfg.compression,
                        use_dictionary=True,
                    )

                writer.write_table(tbl, row_group_size=jcfg.row_group_size)
                bytes_in_current += estimate_row_group_bytes(pf, rg)

                if bytes_in_current >= target_bytes:
                    flush()

        flush()
        return out_written
    finally:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass


def process_one_day(
    client: Minio,
    mcfg: MinioConfig,
    jcfg: JobConfig,
    yyyymmdd: str,
    object_names: List[str],
) -> Tuple[str, int]:
    if not object_names:
        return yyyymmdd, 0

    if jcfg.enable_compaction:
        cnt = rewrite_day_compacted_target_mb(client, mcfg, jcfg, yyyymmdd, object_names)
    else:
        cnt = rewrite_day_one_to_one(client, mcfg, jcfg, yyyymmdd, object_names)

    return yyyymmdd, cnt


# ---------------------------
# CLI
# ---------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", required=True, help="MinIO endpoint host:port")
    ap.add_argument("--access-key", required=True)
    ap.add_argument("--secret-key", required=True)
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--secure", default="false", choices=["true", "false"])

    ap.add_argument("--base-path", required=True, help="Prefix in bucket, e.g. base_path")
    ap.add_argument("--max-workers", type=int, default=1, help="Parallel day workers (default 1)")

    ap.add_argument("--enable-compaction", action="store_true")
    ap.add_argument("--target-mb", type=int, default=50, help="Target output parquet size (approx MB)")
    ap.add_argument("--compression", default="snappy", choices=["snappy", "zstd", "gzip", "brotli"])
    ap.add_argument("--row-group-size", type=int, default=1_048_576)

    ap.add_argument("--skip-if-exists", action="store_true", help="Idempotent: skip destination objects that already exist")

    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    mcfg = MinioConfig(
        endpoint=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
        secure=(args.secure.lower() == "true"),
        bucket=args.bucket,
    )
    jcfg = JobConfig(
        base_path=norm_prefix(args.base_path),
        columns=HARDCODED_COLUMNS,
        enable_compaction=bool(args.enable_compaction),
        target_file_mb=int(args.target_mb),
        row_group_size=int(args.row_group_size),
        compression=str(args.compression),
        skip_if_exists=bool(args.skip_if_exists),
        max_workers=int(args.max_workers),
    )

    client = Minio(
        mcfg.endpoint,
        access_key=mcfg.access_key,
        secret_key=mcfg.secret_key,
        secure=mcfg.secure,
    )

    by_day: Dict[str, List[str]] = {}
    for yyyymmdd, obj_name in iter_history_parquet_objects(client, mcfg.bucket, jcfg.base_path):
        by_day.setdefault(yyyymmdd, []).append(obj_name)

    days = sorted(by_day.keys())
    logging.info("Discovered %d day folders under %s/history/", len(days), jcfg.base_path)

    if jcfg.max_workers <= 1:
        for d in days:
            day, out_cnt = process_one_day(client, mcfg, jcfg, d, by_day[d])
            logging.info("Done day=%s wrote_outputs=%d", day, out_cnt)
    else:
        # Keep configurable because some MinIO envs throttle concurrency [memory:21]
        with cf.ThreadPoolExecutor(max_workers=jcfg.max_workers) as ex:
            futs = [ex.submit(process_one_day, client, mcfg, jcfg, d, by_day[d]) for d in days]
            for f in cf.as_completed(futs):
                day, out_cnt = f.result()
                logging.info("Done day=%s wrote_outputs=%d", day, out_cnt)


if __name__ == "__main__":
    try:
        main()
    except S3Error as e:
        raise SystemExit(f"MinIO error: {e}") from e
```

## How “skip if exists” behaves
- **One-to-one mode:** if `partitioned_dt=.../<same_filename>` already exists, it skips that file.[1]
- **Compaction mode:** writes `part-00000.parquet`, `part-00001.parquet`, etc.; with `--skip-if-exists`, it finds the first free index and also skips any already-existing indices it encounters.[1]

If you want compaction to be restartable *and deterministic* (same exact output file names on rerun), the next improvement would be: compute a stable manifest (hash of input object names + etags) and write to `part-<manifest_hash>-00000.parquet` style names. Should that be added?

[1](https://python.hotexamples.com/examples/minio/Minio/stat_object/python-minio-stat_object-method-examples.html)
[2](https://www.mungingdata.com/pyarrow/parquet-metadata-min-max-statistics/)
[3](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_metadata.html)
[4](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.FileMetaData.html)
[5](https://stackoverflow.com/questions/75196568/how-do-i-get-page-level-data-of-a-parquet-file-with-pyarrow)
[6](https://stackoverflow.com/questions/52122674/how-to-write-parquet-metadata-with-pyarrow)
[7](https://kb.databricks.com/python/how-to-retrieve-parquet-file-metadata)
[8](https://posulliv.github.io/posts/parquet-cli/)
[9](https://github.com/minio/minio-java/issues/639)
[10](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.RowGroupMetaData.html)
[11](http://www.openkb.info/2021/02/how-to-use-pyarrow-to-view-metadata.html)
[12](https://stackoverflow.com/questions/72611255/how-to-check-if-object-exist-in-minio-bucket-using-minio-java-sdk)
[13](https://stackoverflow.com/questions/78646608/estimating-the-size-of-data-when-loaded-from-parquet-file-into-an-arrow-table)
[14](https://skeptric.com/notebooks/reading_parquet_metadata.html)
[15](https://github.com/minio/minio-py/issues/701)
[16](https://github.com/apache/arrow/issues/19902)
[17](https://github.com/apache/arrow/blob/main/python/pyarrow/parquet/core.py)
[18](https://docs.min.io/enterprise/aistor-object-store/developers/sdk/python/api/)
[19](https://github.com/pola-rs/polars/issues/10108)
[20](https://wesm.github.io/arrow-site-test/python/generated/pyarrow.parquet.ParquetFile.html)
