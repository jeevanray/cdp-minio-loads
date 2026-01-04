import logging
import uuid
from datetime import datetime, timedelta
from trino.dbapi import connect
from trino.exceptions import TrinoQueryError

class TrinoManager:
    def __init__(self, conn_params):
        self.conn = connect(**conn_params)
        self.cursor = self.conn.cursor()
        self.logger = logging.getLogger("TrinoManager")

    def execute_query(self, sql):
        self.logger.info(f"Executing SQL: {sql[:200]}...")
        self.cursor.execute(sql)
        try:
            return self.cursor.fetchall()
        except Exception:
            return None

    def get_watermark(self, job_name, source_table):
        query = f"""
            SELECT watermark_value FROM audit.pipeline_state
            WHERE job_name = '{job_name}' 
              AND source_table = '{source_table}'
              AND status = 'COMPLETED'
            ORDER BY updated_at DESC LIMIT 1
        """
        result = self.execute_query(query)
        return result[0][0] if result else '1970-01-01 00:00:00'

    def acquire_lock(self, job_name, stale_hours=2):
        # Check for active locks
        query = f"""
            SELECT run_id, updated_at FROM audit.pipeline_state
            WHERE job_name = '{job_name}' AND status = 'RUNNING'
            AND updated_at > current_timestamp - INTERVAL '{stale_hours}' HOUR
        """
        active_locks = self.execute_query(query)
        if active_locks:
            raise Exception(f"Job {job_name} is currently locked by run {active_locks[0][0]}")

    def update_state(self, run_id, job_name, stage, source, target, status, watermark):
        sql = f"""
            INSERT INTO audit.pipeline_state (run_id, job_name, stage_name, source_table, target_table, status, watermark_value, updated_at)
            VALUES ('{run_id}', '{job_name}', '{stage}', '{source}', '{target}', '{status}', '{watermark}', current_timestamp)
        """
        self.execute_query(sql)

    def get_common_columns(self, source_table, target_table):
        """Fetches intersection of columns to support additive schema evolution."""
        def fetch_cols(table):
            parts = table.split('.')
            query = f"SELECT column_name FROM {parts[0]}.information_schema.columns WHERE table_schema='{parts[1]}' AND table_name='{parts[2]}'"
            return {row[0].lower() for row in self.execute_query(query)}
        
        common = fetch_cols(source_table).intersection(fetch_cols(target_table))
        return ", ".join(list(common))

    def build_merge_query(self, target, source, pks, column_mappings):
        """
        column_mappings: dict {target_col: (source_col, cast_type or None)}
        """
        # Build select list for source with casts/aliases
        select_exprs = []
        for tgt, (src, cast) in column_mappings.items():
            if cast:
                select_exprs.append(f"CAST({src} AS {cast}) AS {tgt}")
            else:
                select_exprs.append(f"{src} AS {tgt}")
        select_sql = ", ".join(select_exprs)

        # Build update set and insert columns/values
        update_set = ", ".join([
            f"T.{tgt} = S.{tgt}" for tgt in column_mappings if tgt not in pks
        ])
        join_cond = " AND ".join([f"T.{pk} = S.{pk}" for pk in pks])
        columns = ", ".join(column_mappings.keys())
        values = ", ".join([f"S.{tgt}" for tgt in column_mappings.keys()])

        return f"""
            MERGE INTO {target} T USING (
                SELECT {select_sql} FROM {source}
            ) S
            ON ({join_cond})
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})
        """

    def get_column_mappings(self, config):
        """
        Returns a dict: {target_col: (source_col, cast_type or None)}
        """
        mappings = config.get("final_table", {}).get("column_mappings", {})
        result = {}
        for tgt, v in mappings.items():
            src = v["source"]
            cast = v.get("cast")
            result[tgt] = (src, cast)
        return result