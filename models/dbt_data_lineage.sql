{{ config(materialized='table') }}

-- DBT Data Lineage Tracking
-- Maps dependencies and data flow through the pipeline

WITH lineage_mapping AS (
    SELECT
        -- Source to staging relationships
        'airbyte_raw_customers' as source_table,
        'sami_test' as source_dataset,
        'stg_customers' as target_model,
        'staging' as target_layer,
        'direct' as relationship_type,
        1 as dependency_level,
        ARRAY['customer_id', 'customer_type', 'email'] as key_columns
        
    UNION ALL SELECT 'airbyte_raw_transactions', 'sami_test', 'stg_transactions', 'staging', 'direct', 1, 
                    ARRAY['transaction_id', 'amount', 'status']
    UNION ALL SELECT 'airbyte_raw_payment_methods', 'sami_test', 'stg_payment_methods', 'staging', 'direct', 1,
                    ARRAY['payment_method_id', 'method_type']
    UNION ALL SELECT 'airbyte_raw_fees', 'sami_test', 'stg_fees', 'staging', 'direct', 1,
                    ARRAY['transaction_id', 'fee_amount']
    UNION ALL SELECT 'airbyte_raw_refunds', 'sami_test', 'stg_refunds', 'staging', 'direct', 1,
                    ARRAY['transaction_id', 'refund_amount']
    
    -- Staging to dimension relationships            
    UNION ALL SELECT 'stg_customers', 'payments_v1', 'dim_customers', 'dimensions', 'transform', 2,
                    ARRAY['customer_id', 'customer_type', 'risk_profile']
    UNION ALL SELECT 'stg_payment_methods', 'payments_v1', 'dim_payment_methods', 'dimensions', 'transform', 2,
                    ARRAY['payment_method_id', 'method_category']
    UNION ALL SELECT 'date_spine', 'utility', 'dim_date', 'dimensions', 'generate', 2,
                    ARRAY['date_day', 'year', 'month']
    
    -- Staging to fact relationships
    UNION ALL SELECT 'stg_transactions', 'payments_v1', 'fact_transactions', 'facts', 'transform', 3,
                    ARRAY['transaction_id', 'amount', 'fees']
    UNION ALL SELECT 'stg_fees', 'payments_v1', 'fact_transactions', 'facts', 'join', 3,
                    ARRAY['transaction_id', 'fee_amount']
    UNION ALL SELECT 'stg_refunds', 'payments_v1', 'fact_transactions', 'facts', 'join', 3,
                    ARRAY['transaction_id', 'refund_amount']
    
    -- Fact to mart relationships
    UNION ALL SELECT 'fact_transactions', 'payments_v1', 'mart_payment_analytics', 'marts', 'star_join', 4,
                    ARRAY['transaction_id', 'gross_amount', 'total_fees']
    UNION ALL SELECT 'dim_customers', 'payments_v1', 'mart_payment_analytics', 'marts', 'star_join', 4,
                    ARRAY['customer_id', 'customer_type', 'risk_profile']
    UNION ALL SELECT 'dim_payment_methods', 'payments_v1', 'mart_payment_analytics', 'marts', 'star_join', 4,
                    ARRAY['payment_method_id', 'method_category']
    UNION ALL SELECT 'dim_date', 'payments_v1', 'mart_payment_analytics', 'marts', 'star_join', 4,
                    ARRAY['date_day', 'day_name', 'month_name']
),

lineage_with_metadata AS (
    SELECT 
        *,
        CURRENT_TIMESTAMP() as lineage_captured_at,
        -- Impact analysis
        CASE dependency_level
            WHEN 1 THEN 'Source Impact: Changes affect staging layer'
            WHEN 2 THEN 'Staging Impact: Changes affect dimensions/facts'
            WHEN 3 THEN 'Dimension Impact: Changes affect star schema'
            WHEN 4 THEN 'Fact Impact: Changes affect business analytics'
        END as impact_description,
        
        -- Criticality scoring
        CASE 
            WHEN target_model = 'mart_payment_analytics' THEN 'HIGH'
            WHEN target_layer IN ('facts', 'dimensions') THEN 'MEDIUM'
            ELSE 'LOW'
        END as criticality_level
        
    FROM lineage_mapping
)

SELECT 
    *,
    -- Generate dependency path
    CONCAT(source_dataset, '.', source_table, ' → ', 'payments_v1.', target_model) as dependency_path,
    
    -- Data flow direction
    CASE relationship_type
        WHEN 'direct' THEN '📥 Extract'
        WHEN 'transform' THEN '🔄 Transform'
        WHEN 'join' THEN '🔗 Join'
        WHEN 'star_join' THEN '⭐ Star Schema'
        WHEN 'generate' THEN '🏭 Generate'
    END as flow_type_icon

FROM lineage_with_metadata
ORDER BY dependency_level, target_model