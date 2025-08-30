{{ config(materialized='view') }}

WITH source_data AS (
    SELECT 
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_loaded_at,
        PARSE_JSON(_airbyte_data) AS data
    FROM {{ source('sami_test_raw', 'airbyte_raw_refunds') }}
    WHERE _airbyte_data IS NOT NULL
),

parsed_data AS (
    SELECT
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_loaded_at,
        
        -- Refund fields (assuming similar structure to transactions)
        JSON_EXTRACT_SCALAR(data, '$.refund_id') AS refund_id,
        JSON_EXTRACT_SCALAR(data, '$.transaction_id') AS transaction_id,
        CAST(JSON_EXTRACT_SCALAR(data, '$.amount') AS NUMERIC) AS amount,
        JSON_EXTRACT_SCALAR(data, '$.currency') AS currency,
        JSON_EXTRACT_SCALAR(data, '$.status') AS status,
        JSON_EXTRACT_SCALAR(data, '$.reason') AS reason,
        
        -- Timestamps
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(data, '$.created_at')) AS created_at,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(data, '$.updated_at')) AS updated_at
        
    FROM source_data
)

SELECT * FROM parsed_data