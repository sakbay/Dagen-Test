{{ config(materialized='incremental', unique_key='run_id') }}

-- DBT Run Results Observability Model
-- Captures execution metadata for pipeline monitoring

WITH run_metadata AS (
    SELECT
        -- Run identification
        GENERATE_UUID() as run_id,
        CURRENT_TIMESTAMP() as run_timestamp,
        'dagen_test' as project_name,
        'dev' as target_name,
        
        -- Environment metadata
        'BigQuery' as warehouse_type,
        'prd-dagen' as project_id,
        'payments_v1' as dataset_name,
        
        -- Pipeline metadata
        'star_schema_pipeline' as pipeline_type,
        ARRAY['staging', 'dimensions', 'facts', 'marts'] as model_layers,
        
        -- Execution context
        CURRENT_USER() as executed_by,
        'scheduled' as execution_type,
        'development' as environment
),

model_inventory AS (
    SELECT 
        'stg_customers' as model_name, 'staging' as layer, 'view' as materialization, 1 as execution_order
    UNION ALL SELECT 'stg_transactions', 'staging', 'view', 2
    UNION ALL SELECT 'stg_payment_methods', 'staging', 'view', 3
    UNION ALL SELECT 'stg_fees', 'staging', 'view', 4
    UNION ALL SELECT 'stg_refunds', 'staging', 'view', 5
    UNION ALL SELECT 'dim_customers', 'dimensions', 'table', 6
    UNION ALL SELECT 'dim_payment_methods', 'dimensions', 'table', 7
    UNION ALL SELECT 'dim_date', 'dimensions', 'table', 8
    UNION ALL SELECT 'fact_transactions', 'facts', 'table', 9
    UNION ALL SELECT 'mart_payment_analytics', 'marts', 'table', 10
)

SELECT 
    rm.*,
    mi.model_name,
    mi.layer,
    mi.materialization,
    mi.execution_order
FROM run_metadata rm
CROSS JOIN model_inventory mi

{% if is_incremental() %}
WHERE run_timestamp > (SELECT MAX(run_timestamp) FROM {{ this }})
{% endif %}