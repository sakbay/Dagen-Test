{{ config(materialized='view') }}

WITH source_data AS (
    SELECT 
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_loaded_at,
        PARSE_JSON(_airbyte_data) AS data
    FROM {{ source('sami_test_raw', 'airbyte_raw_payment_methods') }}
),

parsed_data AS (
    SELECT
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_loaded_at,
        
        -- Payment method fields
        JSON_EXTRACT_SCALAR(data, '$.payment_method_id') AS payment_method_id,
        JSON_EXTRACT_SCALAR(data, '$.customer_id') AS customer_id,
        JSON_EXTRACT_SCALAR(data, '$.method_type') AS method_type,
        JSON_EXTRACT_SCALAR(data, '$.details') AS details_json,
        CAST(JSON_EXTRACT_SCALAR(data, '$.is_default') AS BOOLEAN) AS is_default,
        
        -- Timestamps
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(data, '$.created_at')) AS created_at,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(data, '$.updated_at')) AS updated_at
        
    FROM source_data
),

final AS (
    SELECT
        payment_method_id,
        customer_id,
        method_type,
        is_default,
        
        -- Parse details JSON based on method type
        CASE 
            WHEN method_type IN ('credit_card', 'card') THEN 
                JSON_EXTRACT_SCALAR(PARSE_JSON(details_json), '$.card_number')
            ELSE NULL
        END AS masked_card_number,
        
        CASE 
            WHEN method_type IN ('credit_card', 'card') THEN 
                JSON_EXTRACT_SCALAR(PARSE_JSON(details_json), '$.expiration_date')
            ELSE NULL
        END AS card_expiration_date,
        
        CASE 
            WHEN method_type = 'card' THEN 
                JSON_EXTRACT_SCALAR(PARSE_JSON(details_json), '$.bank_code')
            ELSE NULL
        END AS bank_code,
        
        CASE 
            WHEN method_type = 'card' THEN 
                JSON_EXTRACT_SCALAR(PARSE_JSON(details_json), '$.account_number')
            ELSE NULL
        END AS masked_account_number,
        
        created_at,
        updated_at,
        _airbyte_extracted_at,
        _airbyte_loaded_at
        
    FROM parsed_data
)

SELECT * FROM final