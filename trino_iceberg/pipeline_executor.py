import json
import logging
import uuid
from trino_manager import TrinoManager

logging.basicConfig(level=logging.INFO)

def run_pipeline(config_path, trino_conn_details):
    with open(config_path) as f:
        config = json.load(f)
    
    manager = TrinoManager(trino_conn_details)
    run_id = str(uuid.uuid4())

    for job in config['jobs']:
        job_name = job['job_name']
        try:
            manager.acquire_lock(job_name)
            
            for src in job['sources']:
                source_table = src['source_table']
                target_table = job['target_table_name']
                watermark = manager.get_watermark(job_name, source_table)
                
                # STAGE 1: Extract to Staging
                if src.get('use_stage'):
                    manager.update_state(run_id, job_name, "STAGING", source_table, src['staging_table'], "RUNNING", watermark)
                    
                    # Additive Column Fetching
                    cols = manager.get_common_columns(source_table, src['staging_table'])
                    extract_sql = src['extract_sql'].format(columns=cols, watermark=watermark)
                    
                    manager.execute_query(f"TRUNCATE TABLE {src['staging_table']}")
                    manager.execute_query(f"INSERT INTO {src['staging_table']} ({cols}) {extract_sql}")
                    
                    manager.update_state(run_id, job_name, "STAGING", source_table, src['staging_table'], "COMPLETED", watermark)
                
                # STAGE 2: Merge into Final Iceberg Table
                current_source = src['staging_table'] if src.get('use_stage') else source_table
                manager.update_state(run_id, job_name, "FINAL_MERGE", current_source, target_table, "RUNNING", watermark)
                
                cols = manager.get_common_columns(current_source, target_table)
                merge_sql = manager.build_merge_query(target_table, current_source, job['primary_keys'], cols)
                
                manager.execute_query(merge_sql)
                
                # Update watermark to the latest from the source for this run
                new_watermark_query = f"SELECT MAX({src['watermark_column']}) FROM {current_source}"
                new_watermark = manager.execute_query(new_watermark_query)[0][0]
                
                manager.update_state(run_id, job_name, "FINAL_MERGE", current_source, target_table, "COMPLETED", str(new_watermark))

        except Exception as e:
            logging.error(f"Job {job_name} failed: {e}")
            manager.update_state(run_id, job_name, "ERROR", "N/A", "N/A", "FAILED", "N/A")

if __name__ == "__main__":
    # Example Airflow usage: Pass connection details from Airflow Hooks
    conn_info = {"host": "localhost", "port": 8080, "user": "admin", "catalog": "iceberg"}
    run_pipeline("config.json", conn_info)