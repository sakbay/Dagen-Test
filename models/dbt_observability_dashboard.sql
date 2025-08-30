{{ config(materialized='table') }}

-- DBT Observability Dashboard
-- Comprehensive monitoring view for data engineers

WITH pipeline_health AS (
    SELECT
        'PIPELINE HEALTH OVERVIEW' as section,
        NULL as metric,
        NULL as value,
        NULL as status,
        NULL as trend,
        NULL as alert_level
        
    UNION ALL
    
    -- Model performance summary
    SELECT 
        'Performance',
        'Models Running Optimally',
        CONCAT(COUNT(CASE WHEN performance_status = '🟢 Fast' THEN 1 END), ' of ', COUNT(*)),
        CASE 
            WHEN COUNT(CASE WHEN performance_status = '🟢 Fast' THEN 1 END) = COUNT(*) THEN '🟢 Excellent'
            WHEN COUNT(CASE WHEN performance_status = '🟢 Fast' THEN 1 END) >= COUNT(*) * 0.8 THEN '🟡 Good'
            ELSE '🔴 Poor'
        END,
        '📈 Stable',
        CASE WHEN COUNT(CASE WHEN performance_status = '🔴 Slow' THEN 1 END) > 0 THEN 'HIGH' ELSE 'LOW' END
    FROM {{ ref('dbt_model_performance') }}
    
    UNION ALL
    
    -- Data quality summary  
    SELECT
        'Data Quality',
        'Models with Excellent Quality',
        CONCAT(COUNT(CASE WHEN quality_status = '🟢 Excellent' THEN 1 END), ' of ', COUNT(*)),
        CASE 
            WHEN COUNT(CASE WHEN quality_status = '🟢 Excellent' THEN 1 END) = COUNT(*) THEN '🟢 Excellent'
            WHEN COUNT(CASE WHEN quality_status = '🟢 Excellent' THEN 1 END) >= COUNT(*) * 0.8 THEN '🟡 Good'
            ELSE '🔴 Poor'
        END,
        '📊 Improving',
        CASE WHEN COUNT(CASE WHEN quality_status = '🔴 Poor' THEN 1 END) > 0 THEN 'CRITICAL' ELSE 'LOW' END
    FROM {{ ref('dbt_model_performance') }}
    
    UNION ALL
    
    -- Data freshness summary
    SELECT
        'Data Freshness',
        'Fresh Data Sources',
        CONCAT(COUNT(CASE WHEN freshness_status = '🟢 Fresh' THEN 1 END), ' of ', COUNT(*)),
        CASE 
            WHEN COUNT(CASE WHEN freshness_status = '🟢 Fresh' THEN 1 END) = COUNT(*) THEN '🟢 Fresh'
            WHEN COUNT(CASE WHEN freshness_status = '🟡 Stale' THEN 1 END) > 0 THEN '🟡 Some Stale'
            ELSE '🔴 Stale Data'
        END,
        '⏱️ Monitoring',
        CASE WHEN COUNT(CASE WHEN freshness_status = '🔴 Very Stale' THEN 1 END) > 0 THEN 'HIGH' ELSE 'MEDIUM' END
    FROM {{ ref('dbt_model_performance') }}
),

model_details AS (
    SELECT
        'MODEL PERFORMANCE DETAILS' as section,
        NULL as metric,
        NULL as value, 
        NULL as status,
        NULL as trend,
        NULL as alert_level
        
    UNION ALL
    
    SELECT
        'Model Details',
        CONCAT(model_name, ' (', layer, ')'),
        CONCAT(CAST(row_count as STRING), ' rows, ', CAST(execution_time_seconds as STRING), 's'),
        CONCAT(performance_status, ' ', quality_status, ' ', freshness_status),
        CASE 
            WHEN execution_time_seconds < 5 THEN '📈 Fast'
            WHEN execution_time_seconds < 15 THEN '📊 Moderate'
            ELSE '📉 Slow'
        END,
        CASE 
            WHEN performance_status = '🔴 Slow' OR quality_status = '🔴 Poor' THEN 'HIGH'
            WHEN freshness_status = '🔴 Very Stale' THEN 'MEDIUM'
            ELSE 'LOW'
        END
    FROM {{ ref('dbt_model_performance') }}
),

lineage_insights AS (
    SELECT
        'DATA LINEAGE INSIGHTS' as section,
        NULL as metric,
        NULL as value,
        NULL as status, 
        NULL as trend,
        NULL as alert_level
        
    UNION ALL
    
    SELECT
        'Critical Dependencies',
        'High Impact Models',
        CAST(COUNT(CASE WHEN criticality_level = 'HIGH' THEN 1 END) as STRING),
        '🎯 Monitored',
        '🔍 Tracking',
        'MEDIUM'
    FROM {{ ref('dbt_data_lineage') }}
    
    UNION ALL
    
    SELECT
        'Dependency Layers',
        'Pipeline Depth',
        CONCAT(CAST(MAX(dependency_level) as STRING), ' layers'),
        '⭐ Star Schema',
        '🏗️ Structured',
        'LOW'
    FROM {{ ref('dbt_data_lineage') }}
),

business_metrics AS (
    SELECT
        'BUSINESS IMPACT METRICS' as section,
        NULL as metric,
        NULL as value,
        NULL as status,
        NULL as trend, 
        NULL as alert_level
        
    UNION ALL
    
    SELECT
        'Data Volume',
        'Total Transactions Processed',
        CAST((SELECT COUNT(*) FROM {{ ref('mart_payment_analytics') }}) as STRING),
        '📊 Active',
        '📈 Growing',
        'LOW'
        
    UNION ALL
    
    SELECT
        'Revenue Impact',
        'Total Processing Volume',
        CONCAT('$', CAST(ROUND((SELECT SUM(gross_amount) FROM {{ ref('mart_payment_analytics') }}), 2) as STRING)),
        '💰 Healthy',
        '📈 Tracking',
        'LOW'
        
    UNION ALL
    
    SELECT
        'Data Quality Impact',
        'Success Rate',
        CONCAT(CAST(ROUND((SELECT SUM(completed_transaction_count) * 100.0 / COUNT(*) FROM {{ ref('mart_payment_analytics') }}), 1) as STRING), '%'),
        CASE 
            WHEN (SELECT SUM(completed_transaction_count) * 100.0 / COUNT(*) FROM {{ ref('mart_payment_analytics') }}) > 80 THEN '🟢 Good'
            WHEN (SELECT SUM(completed_transaction_count) * 100.0 / COUNT(*) FROM {{ ref('mart_payment_analytics') }}) > 60 THEN '🟡 Fair'
            ELSE '🔴 Poor'
        END,
        '⚠️ Needs Attention',
        'HIGH'
)

SELECT * FROM pipeline_health
UNION ALL SELECT * FROM model_details  
UNION ALL SELECT * FROM lineage_insights
UNION ALL SELECT * FROM business_metrics
ORDER BY 
    CASE section
        WHEN 'PIPELINE HEALTH OVERVIEW' THEN 1
        WHEN 'MODEL PERFORMANCE DETAILS' THEN 2
        WHEN 'DATA LINEAGE INSIGHTS' THEN 3
        WHEN 'BUSINESS IMPACT METRICS' THEN 4
        ELSE 5
    END,
    metric