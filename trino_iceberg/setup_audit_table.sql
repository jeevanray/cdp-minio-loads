CREATE TABLE IF NOT EXISTS audit.pipeline_state (
    run_id VARCHAR,
    job_name VARCHAR,
    stage_name VARCHAR,
    source_table VARCHAR,
    target_table VARCHAR,
    status VARCHAR, -- 'RUNNING', 'COMPLETED', 'FAILED'
    watermark_value VARCHAR,
    updated_at TIMESTAMP(6) WITH TIME ZONE
) 
WITH (
    format = 'ICEBERG',
    partitioning = ARRAY['job_name']
);