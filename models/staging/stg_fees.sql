{{ config(materialized='view') }}

WITH source_data AS (
    SELECT 
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_loaded_at,
        PARSE_JSON(_airbyte_data) AS data
    FROM {{ source('sami_test_raw', 'airbyte_raw_fees') }}
),

parsed_data AS (
    SELECT
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_loaded_at,
        
        -- Fee fields
        JSON_EXTRACT_SCALAR(data, '$.fee_id') AS fee_id,
        JSON_EXTRACT_SCALAR(data, '$.transaction_id') AS transaction_id,
        CAST(JSON_EXTRACT_SCALAR(data, '$.amount') AS NUMERIC) AS amount,
        JSON_EXTRACT_SCALAR(data, '$.currency') AS currency,
        JSON_EXTRACT_SCALAR(data, '$.fee_type') AS fee_type,
        
        -- Timestamps
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(data, '$.created_at')) AS created_at
        
    FROM source_data
)

SELECT * FROM parsed_data