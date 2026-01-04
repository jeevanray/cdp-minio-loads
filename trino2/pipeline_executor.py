import json
import uuid
import logging
from trino_manager import TrinoManager

logging.basicConfig(level=logging.INFO)

def run_pipeline():
    # 1. Setup
    with open('config.json') as f:
        config = json.load(f)
    
    # Update these with your real Trino/Minio credentials
    trino_conn = {"host": "your-trino-host", "port": 8080, "user": "admin", "catalog": "minio"}
    manager = TrinoManager(trino_conn)
    run_id = str(uuid.uuid4())

    for job in config['jobs']:
        job_name = job['job_name']
        manager.acquire_lock(job_name)
        
        # 2. Dynamic Discovery
        campaigns = manager.get_campaigns(job['source']['table'], job['source']['app_id'])
        
        for campaign in campaigns:
            status, watermark = manager.get_last_state(job_name, campaign)
            
            if status == 'COMPLETED':
                logging.info(f"Skipping campaign {campaign}, already processed.")
                continue
            
            logging.info(f"Processing campaign: {campaign} from watermark: {watermark}")
            manager.update_audit(run_id, job_name, campaign, "RUNNING", watermark)

            try:
                # 3. Dynamic Schema resolution
                target_cols = manager.get_columns(job['target_table_name'])
                
                # 4. Prepare and Run Merge
                source_sql = job['source']['extract_sql'].format(campaign_code=campaign, watermark=watermark)
                merge_sql = manager.build_merge_sql(job['target_table_name'], source_sql, job['primary_keys'], target_cols)
                
                manager.execute(merge_sql)
                
                # 5. Calculate new watermark (Max timestamp from this run)
                # Note: In a real scenario, you'd query the source for the max timestamp just processed
                manager.update_audit(run_id, job_name, campaign, "COMPLETED", "CURRENT_BATCH_MAX")
                
            except Exception as e:
                manager.update_audit(run_id, job_name, campaign, "FAILED", watermark)
                logging.error(f"Failed at campaign {campaign}: {str(e)}")
                raise # Stop the pipeline so it doesn't process next campaign in error

if __name__ == "__main__":
    run_pipeline()
