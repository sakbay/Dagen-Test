{{ config(materialized='table') }}

WITH payment_methods AS (
    SELECT
        payment_method_id,
        customer_id,
        method_type,
        is_default,
        masked_card_number,
        card_expiration_date,
        bank_code,
        masked_account_number,
        created_at,
        updated_at,
        
        -- Add derived fields
        CASE 
            WHEN method_type IN ('credit_card', 'card') THEN 'Card'
            WHEN method_type = 'bank_transfer' THEN 'Bank Transfer'
            WHEN method_type = 'digital_wallet' THEN 'Digital Wallet'
            ELSE 'Other'
        END AS method_category,
        
        -- Extract card brand from masked number (simplified logic)
        CASE 
            WHEN masked_card_number LIKE '%4%' THEN 'Visa'
            WHEN masked_card_number LIKE '%5%' THEN 'Mastercard'
            WHEN masked_card_number LIKE '%3%' THEN 'Amex'
            ELSE 'Unknown'
        END AS card_brand,
        
        -- Check if card is expired (simplified - assumes current date)
        CASE 
            WHEN card_expiration_date IS NOT NULL 
                 AND PARSE_DATE('%m/%y', card_expiration_date) < CURRENT_DATE() 
            THEN TRUE
            ELSE FALSE
        END AS is_expired,
        
        ROW_NUMBER() OVER (PARTITION BY payment_method_id ORDER BY updated_at DESC) AS row_num
        
    FROM {{ ref('stg_payment_methods') }}
)

SELECT
    payment_method_id,
    customer_id,
    method_type,
    method_category,
    is_default,
    masked_card_number,
    card_expiration_date,
    card_brand,
    is_expired,
    bank_code,
    masked_account_number,
    created_at,
    updated_at
FROM payment_methods
WHERE row_num = 1  -- Get latest version of each payment method