{{ config(materialized='table') }}

WITH transaction_base AS (
    SELECT
        transaction_id,
        amount,
        currency,
        status,
        payment_method_id,
        debtor_customer_id,
        creditor_customer_id,
        reference,
        created_at,
        updated_at,
        DATE(created_at) AS transaction_date,
        
        -- Add derived metrics
        CASE 
            WHEN status = 'completed' THEN amount
            ELSE 0
        END AS completed_amount,
        
        CASE 
            WHEN status = 'completed' THEN 1
            ELSE 0
        END AS completed_transaction_count,
        
        CASE 
            WHEN status = 'failed' THEN 1
            ELSE 0
        END AS failed_transaction_count,
        
        CASE 
            WHEN status = 'pending' THEN 1
            ELSE 0
        END AS pending_transaction_count
        
    FROM {{ ref('stg_transactions') }}
),

transaction_fees AS (
    SELECT
        transaction_id,
        SUM(amount) AS total_fees,
        COUNT(*) AS fee_count,
        STRING_AGG(fee_type, ', ') AS fee_types
    FROM {{ ref('stg_fees') }}
    GROUP BY transaction_id
),

transaction_refunds AS (
    SELECT
        transaction_id,
        SUM(amount) AS total_refunds,
        COUNT(*) AS refund_count
    FROM {{ ref('stg_refunds') }}
    WHERE status = 'completed'  -- Only count completed refunds
    GROUP BY transaction_id
),

final AS (
    SELECT
        t.transaction_id,
        t.amount AS gross_amount,
        t.currency,
        t.status,
        t.payment_method_id,
        t.debtor_customer_id,
        t.creditor_customer_id,
        t.reference,
        t.transaction_date,
        t.created_at,
        t.updated_at,
        
        -- Fee information
        COALESCE(f.total_fees, 0) AS total_fees,
        COALESCE(f.fee_count, 0) AS fee_count,
        f.fee_types,
        
        -- Refund information
        COALESCE(r.total_refunds, 0) AS total_refunds,
        COALESCE(r.refund_count, 0) AS refund_count,
        
        -- Calculated metrics
        t.amount - COALESCE(f.total_fees, 0) AS net_amount,
        t.amount - COALESCE(r.total_refunds, 0) AS amount_after_refunds,
        t.amount - COALESCE(f.total_fees, 0) - COALESCE(r.total_refunds, 0) AS final_net_amount,
        
        -- Status flags
        t.completed_amount,
        t.completed_transaction_count,
        t.failed_transaction_count,
        t.pending_transaction_count,
        
        -- Additional flags
        CASE WHEN t.debtor_customer_id = t.creditor_customer_id THEN TRUE ELSE FALSE END AS is_self_transaction,
        CASE WHEN COALESCE(r.refund_count, 0) > 0 THEN TRUE ELSE FALSE END AS has_refunds,
        CASE WHEN COALESCE(f.fee_count, 0) > 0 THEN TRUE ELSE FALSE END AS has_fees,
        
        -- Time-based metrics
        DATE_DIFF(CURRENT_DATE(), t.transaction_date, DAY) AS days_since_transaction,
        EXTRACT(HOUR FROM t.created_at) AS transaction_hour,
        EXTRACT(DAYOFWEEK FROM t.created_at) AS transaction_day_of_week
        
    FROM transaction_base t
    LEFT JOIN transaction_fees f ON t.transaction_id = f.transaction_id
    LEFT JOIN transaction_refunds r ON t.transaction_id = r.transaction_id
)

SELECT * FROM final