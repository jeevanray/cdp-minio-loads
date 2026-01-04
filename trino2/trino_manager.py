import logging
from trino.dbapi import connect

class TrinoManager:
    def __init__(self, conn_params):
        self.conn = connect(**conn_params)
        self.cursor = self.conn.cursor()
        self.logger = logging.getLogger("TrinoManager")

    def execute(self, sql):
        self.logger.info(f"Executing: {sql[:150]}...")
        self.cursor.execute(sql)
        try:
            return self.cursor.fetchall()
        except:
            return None

    def get_campaigns(self, source_table, app_id):
        """Dynamically extract campaign codes in ascending order."""
        sql = f"SELECT DISTINCT CAMPAIGN_CODE FROM {source_table} WHERE SOURCE_APP = {app_id} ORDER BY CAMPAIGN_CODE ASC"
        res = self.execute(sql)
        return [r[0] for r in res] if res else []

    def get_last_state(self, job_name, campaign_code):
        """Check if campaign is done or get the last watermark."""
        sql = f"""
            SELECT status, watermark_value FROM audit.pipeline_state 
            WHERE job_name = '{job_name}' AND campaign_code = '{campaign_code}'
            ORDER BY updated_at DESC LIMIT 1
        """
        res = self.execute(sql)
        if res:
            return res[0][0], res[0][1] # status, watermark
        return None, '1970-01-01 00:00:00'

    def acquire_lock(self, job_name):
        sql = f"SELECT run_id FROM audit.pipeline_state WHERE job_name='{job_name}' AND status='RUNNING' AND updated_at > current_timestamp - INTERVAL '2' HOUR"
        if self.execute(sql):
            raise Exception(f"Job {job_name} is locked by another process.")

    def update_audit(self, run_id, job, campaign, status, watermark, stage="FINAL"):
        sql = f"""
            INSERT INTO audit.pipeline_state (run_id, job_name, campaign_code, stage_name, status, watermark_value, updated_at)
            VALUES ('{run_id}', '{job}', '{campaign}', '{stage}', '{status}', '{watermark}', current_timestamp)
        """
        self.execute(sql)

    def get_columns(self, table_name):
        """Supports Additive Schema Evolution by finding common columns."""
        parts = table_name.split('.')
        sql = f"SELECT column_name FROM {parts[0]}.information_schema.columns WHERE table_schema='{parts[1]}' AND table_name='{parts[2]}'"
        return [r[0].lower() for r in self.execute(sql)]

    def build_merge_sql(self, target, source_query, pks, columns):
        update_cols = [c for c in columns if c not in pks]
        set_clause = ", ".join([f"T.{c} = S.{c}" for c in update_cols])
        join_clause = " AND ".join([f"T.{pk} = S.{pk}" for pk in pks])
        col_names = ", ".join(columns)
        val_names = ", ".join([f"S.{c}" for c in columns])

        return f"""
            MERGE INTO {target} T USING ({source_query}) S
            ON {join_clause}
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({col_names}) VALUES ({val_names})
        """
