{{ config(materialized='view') }}

WITH source_data AS (
    SELECT 
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_loaded_at,
        PARSE_JSON(_airbyte_data) AS data
    FROM {{ source('sami_test_raw', 'airbyte_raw_customers') }}
),

parsed_data AS (
    SELECT
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_loaded_at,
        
        -- Customer fields
        JSON_EXTRACT_SCALAR(data, '$.customer_id') AS customer_id,
        JSON_EXTRACT_SCALAR(data, '$.customer_type') AS customer_type,
        JSON_EXTRACT_SCALAR(data, '$.email') AS email,
        JSON_EXTRACT_SCALAR(data, '$.phone_number') AS phone_number,
        JSON_EXTRACT_SCALAR(data, '$.kyc_status') AS kyc_status,
        JSON_EXTRACT_SCALAR(data, '$.risk_profile') AS risk_profile,
        
        -- Address fields (nested JSON)
        JSON_EXTRACT_SCALAR(data, '$.address') AS address_json,
        
        -- Timestamps
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(data, '$.created_at')) AS created_at,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', JSON_EXTRACT_SCALAR(data, '$.updated_at')) AS updated_at
        
    FROM source_data
),

final AS (
    SELECT
        customer_id,
        customer_type,
        email,
        phone_number,
        kyc_status,
        risk_profile,
        
        -- Parse address JSON
        JSON_EXTRACT_SCALAR(PARSE_JSON(address_json), '$.street') AS address_street,
        JSON_EXTRACT_SCALAR(PARSE_JSON(address_json), '$.city') AS address_city,
        JSON_EXTRACT_SCALAR(PARSE_JSON(address_json), '$.state') AS address_state,
        JSON_EXTRACT_SCALAR(PARSE_JSON(address_json), '$.zip') AS address_zip,
        JSON_EXTRACT_SCALAR(PARSE_JSON(address_json), '$.country') AS address_country,
        
        created_at,
        updated_at,
        _airbyte_extracted_at,
        _airbyte_loaded_at
        
    FROM parsed_data
)

SELECT * FROM final