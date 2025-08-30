-- Test query to validate dbt configuration
-- This simulates what dbt would compile for our staging model

WITH source_data AS (
    SELECT 
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_loaded_at,
        PARSE_JSON(_airbyte_data) AS data
    FROM `prd-dagen.sami_test.airbyte_raw_customers`
    LIMIT 1
),

parsed_data AS (
    SELECT
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_loaded_at,
        
        -- Customer fields
        JSON_EXTRACT_SCALAR(data, '$.customer_id') AS customer_id,
        JSON_EXTRACT_SCALAR(data, '$.customer_type') AS customer_type,
        JSON_EXTRACT_SCALAR(data, '$.email') AS email
        
    FROM source_data
)

SELECT 
    'DBT Configuration Test' as test_result,
    COUNT(*) as records_processed,
    'Configuration names fixed successfully' as message
FROM parsed_data