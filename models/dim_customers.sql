{{ config(materialized='table') }}

WITH customers AS (
    SELECT
        customer_id,
        customer_type,
        email,
        phone_number,
        kyc_status,
        risk_profile,
        address_street,
        address_city,
        address_state,
        address_zip,
        address_country,
        created_at,
        updated_at,
        
        -- Add derived fields
        CASE 
            WHEN address_country IS NULL OR address_country = '' THEN 'Unknown'
            ELSE address_country
        END AS country_clean,
        
        CASE 
            WHEN kyc_status = 'verified' THEN TRUE
            ELSE FALSE
        END AS is_verified,
        
        CASE 
            WHEN risk_profile = 'low' THEN 1
            WHEN risk_profile = 'medium' THEN 2
            WHEN risk_profile = 'high' THEN 3
            ELSE 0
        END AS risk_score,
        
        -- Calculate customer age in days
        DATE_DIFF(CURRENT_DATE(), DATE(created_at), DAY) AS customer_age_days,
        
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS row_num
        
    FROM {{ ref('stg_customers') }}
)

SELECT
    customer_id,
    customer_type,
    email,
    phone_number,
    kyc_status,
    risk_profile,
    address_street,
    address_city,
    address_state,
    address_zip,
    address_country,
    country_clean,
    is_verified,
    risk_score,
    customer_age_days,
    created_at,
    updated_at
FROM customers
WHERE row_num = 1  -- Get latest version of each customer