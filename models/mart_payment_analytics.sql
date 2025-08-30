{{ config(materialized='table') }}

-- Star Schema: Payment Analytics Mart
-- Fact Table: Transactions with all related dimensions

SELECT
    -- Transaction Facts (Measures)
    ft.transaction_id,
    ft.gross_amount,
    ft.net_amount,
    ft.final_net_amount,
    ft.total_fees,
    ft.total_refunds,
    ft.completed_amount,
    ft.completed_transaction_count,
    ft.failed_transaction_count,
    ft.pending_transaction_count,
    ft.currency,
    ft.status,
    ft.reference,
    ft.fee_count,
    ft.refund_count,
    ft.is_self_transaction,
    ft.has_refunds,
    ft.has_fees,
    ft.days_since_transaction,
    ft.transaction_hour,
    ft.transaction_day_of_week,
    ft.created_at AS transaction_timestamp,
    ft.updated_at AS transaction_updated_at,
    
    -- Date Dimension
    ft.transaction_date,
    dd.year AS transaction_year,
    dd.month AS transaction_month,
    dd.quarter AS transaction_quarter,
    dd.month_name,
    dd.day_name,
    dd.quarter_label,
    dd.year_month,
    dd.is_weekday,
    dd.is_weekend,
    
    -- Debtor Customer Dimension (Customer who pays)
    dc.customer_id AS debtor_customer_id,
    dc.customer_type AS debtor_customer_type,
    dc.email AS debtor_email,
    dc.kyc_status AS debtor_kyc_status,
    dc.risk_profile AS debtor_risk_profile,
    dc.address_city AS debtor_city,
    dc.address_state AS debtor_state,
    dc.address_country AS debtor_country,
    dc.country_clean AS debtor_country_clean,
    dc.is_verified AS debtor_is_verified,
    dc.risk_score AS debtor_risk_score,
    dc.customer_age_days AS debtor_age_days,
    
    -- Creditor Customer Dimension (Customer who receives)
    cc.customer_id AS creditor_customer_id,
    cc.customer_type AS creditor_customer_type,
    cc.email AS creditor_email,
    cc.kyc_status AS creditor_kyc_status,
    cc.risk_profile AS creditor_risk_profile,
    cc.address_city AS creditor_city,
    cc.address_state AS creditor_state,
    cc.address_country AS creditor_country,
    cc.country_clean AS creditor_country_clean,
    cc.is_verified AS creditor_is_verified,
    cc.risk_score AS creditor_risk_score,
    cc.customer_age_days AS creditor_age_days,
    
    -- Payment Method Dimension
    pm.payment_method_id,
    pm.method_type,
    pm.method_category,
    pm.is_default AS is_default_payment_method,
    pm.card_brand,
    pm.is_expired AS is_payment_method_expired,
    pm.bank_code
    
FROM {{ ref('fact_transactions') }} ft

-- Join with Date Dimension
LEFT JOIN {{ ref('dim_date') }} dd
    ON ft.transaction_date = dd.date_day

-- Join with Debtor Customer Dimension
LEFT JOIN {{ ref('dim_customers') }} dc
    ON ft.debtor_customer_id = dc.customer_id

-- Join with Creditor Customer Dimension  
LEFT JOIN {{ ref('dim_customers') }} cc
    ON ft.creditor_customer_id = cc.customer_id

-- Join with Payment Method Dimension
LEFT JOIN {{ ref('dim_payment_methods') }} pm
    ON ft.payment_method_id = pm.payment_method_id