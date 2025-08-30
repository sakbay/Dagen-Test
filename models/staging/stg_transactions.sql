{{ config(materialized='view') }}

WITH source_data AS (
    SELECT 
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_loaded_at,
        PARSE_JSON(_airbyte_data) AS data
    FROM {{ source('sami_test_raw', 'airbyte_raw_transactions') }}
),

parsed_data AS (
    SELECT
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_loaded_at,
        
        -- Transaction fields
        JSON_EXTRACT_SCALAR(data, '$.transaction_id') AS transaction_id,
        CAST(JSON_EXTRACT_SCALAR(data, '$.amount') AS NUMERIC) AS amount,
        JSON_EXTRACT_SCALAR(data, '$.currency') AS currency,
        JSON_EXTRACT_SCALAR(data, '$.status') AS status,
        JSON_EXTRACT_SCALAR(data, '$.payment_method_id') AS payment_method_id,
        JSON_EXTRACT_SCALAR(data, '$.debtor_customer_id') AS debtor_customer_id,
        JSON_EXTRACT_SCALAR(data, '$.creditor_customer_id') AS creditor_customer_id,
        JSON_EXTRACT_SCALAR(data, '$.reference') AS reference,
        
        -- Timestamps
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(data, '$.created_at')) AS created_at,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(data, '$.updated_at')) AS updated_at
        
    FROM source_data
)

SELECT * FROM parsed_data