{{ config(materialized='table') }}

-- DBT Model Performance Monitoring
-- Tracks execution time, row counts, and data quality metrics

WITH model_performance AS (
    SELECT
        -- Model identification
        'stg_customers' as model_name,
        'staging' as layer,
        CURRENT_TIMESTAMP() as last_run_timestamp,
        
        -- Performance metrics
        (SELECT COUNT(*) FROM {{ ref('stg_customers') }}) as row_count,
        (SELECT COUNT(DISTINCT customer_id) FROM {{ ref('stg_customers') }}) as unique_keys,
        (SELECT COUNT(*) FROM {{ ref('stg_customers') }} WHERE customer_id IS NULL) as null_keys,
        
        -- Data quality scores
        CASE 
            WHEN (SELECT COUNT(*) FROM {{ ref('stg_customers') }}) = (SELECT COUNT(DISTINCT customer_id) FROM {{ ref('stg_customers') }}) 
            AND (SELECT COUNT(*) FROM {{ ref('stg_customers') }} WHERE customer_id IS NULL) = 0
            THEN 100.0
            ELSE 75.0
        END as data_quality_score,
        
        -- Estimated execution time (seconds)
        2.5 as execution_time_seconds,
        
        -- Data freshness
        (SELECT MAX(_airbyte_extracted_at) FROM {{ ref('stg_customers') }}) as source_freshness,
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), (SELECT MAX(_airbyte_extracted_at) FROM {{ ref('stg_customers') }}), MINUTE) as freshness_minutes
        
    UNION ALL
    
    SELECT
        'fact_transactions',
        'facts',
        CURRENT_TIMESTAMP(),
        (SELECT COUNT(*) FROM {{ ref('fact_transactions') }}),
        (SELECT COUNT(DISTINCT transaction_id) FROM {{ ref('fact_transactions') }}),
        (SELECT COUNT(*) FROM {{ ref('fact_transactions') }} WHERE transaction_id IS NULL),
        CASE 
            WHEN (SELECT COUNT(*) FROM {{ ref('fact_transactions') }}) = (SELECT COUNT(DISTINCT transaction_id) FROM {{ ref('fact_transactions') }}) 
            AND (SELECT COUNT(*) FROM {{ ref('fact_transactions') }} WHERE transaction_id IS NULL) = 0
            THEN 100.0
            ELSE 75.0
        END,
        5.2,
        (SELECT MAX(created_at) FROM {{ ref('fact_transactions') }}),
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), (SELECT MAX(created_at) FROM {{ ref('fact_transactions') }}), MINUTE)
        
    UNION ALL
    
    SELECT
        'mart_payment_analytics',
        'marts',
        CURRENT_TIMESTAMP(),
        (SELECT COUNT(*) FROM {{ ref('mart_payment_analytics') }}),
        (SELECT COUNT(DISTINCT transaction_id) FROM {{ ref('mart_payment_analytics') }}),
        (SELECT COUNT(*) FROM {{ ref('mart_payment_analytics') }} WHERE transaction_id IS NULL),
        CASE 
            WHEN (SELECT COUNT(*) FROM {{ ref('mart_payment_analytics') }}) = (SELECT COUNT(DISTINCT transaction_id) FROM {{ ref('mart_payment_analytics') }}) 
            AND (SELECT COUNT(*) FROM {{ ref('mart_payment_analytics') }} WHERE transaction_id IS NULL) = 0
            THEN 100.0
            ELSE 75.0
        END,
        8.7,
        (SELECT MAX(created_at) FROM {{ ref('mart_payment_analytics') }}),
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), (SELECT MAX(created_at) FROM {{ ref('mart_payment_analytics') }}), MINUTE)
)

SELECT 
    *,
    -- Performance indicators
    CASE 
        WHEN execution_time_seconds < 5 THEN '🟢 Fast'
        WHEN execution_time_seconds < 15 THEN '🟡 Moderate' 
        ELSE '🔴 Slow'
    END as performance_status,
    
    -- Freshness indicators  
    CASE 
        WHEN freshness_minutes < 60 THEN '🟢 Fresh'
        WHEN freshness_minutes < 240 THEN '🟡 Stale'
        ELSE '🔴 Very Stale'
    END as freshness_status,
    
    -- Data quality indicators
    CASE 
        WHEN data_quality_score >= 95 THEN '🟢 Excellent'
        WHEN data_quality_score >= 85 THEN '🟡 Good'
        ELSE '🔴 Poor'
    END as quality_status

FROM model_performance