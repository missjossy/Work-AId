-- Original Query Count
WITH payment_transactions AS (
    SELECT 
        rte.loan_key,
        li.loan_id,
        rte.client_id,
        li.disbursementdate,
        rte.installment,
        rte.repayment_due_date,
        rte.transaction_date,
        rte.amount as payment_amount,
        rte.total_due,
        li.penaltypaid,
        CASE 
            WHEN li.LN = 0 THEN 'LN0'
            WHEN li.LN = 1 THEN 'LN1'
            WHEN li.LN = 2 THEN 'LN2'
            WHEN li.LN BETWEEN 3 AND 5 THEN 'LN3-5'
            WHEN li.LN BETWEEN 6 AND 8 THEN 'LN6-8'
            WHEN li.LN BETWEEN 9 AND 11 THEN 'LN9-11'
            ELSE 'LN12+'
        END as ln_group,
        CASE 
            WHEN li.repaymentinstallments = 1 THEN 'regular'
            ELSE 'installments'
        END as loan_type,
        DATEDIFF('day', rte.repayment_due_date, rte.transaction_date) as days_late,
        DATE_TRUNC('month', rte.transaction_date) as payment_month
    FROM ml.repayment_transactions_extended rte
    JOIN ml.loan_info_tbl li ON rte.loan_key = li.loan_key
    WHERE 
        rte.disbursementdate IS NOT NULL
        AND YEAR(rte.disbursementdate) >= 2024
        AND rte.transaction_date > rte.repayment_due_date
        AND rte.amount > 0  -- Only count actual payments
        AND rte.status = 'succeeded'  -- Only count successful payments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY rte.loan_key, rte.installment ORDER BY rte.transaction_date DESC) = 1  -- Take only the latest payment per installment
),

-- Count from original query
original_count AS (
    SELECT 
        ln_group,
        COUNT(DISTINCT client_id) as original_client_count,
        COUNT(DISTINCT loan_id) as original_loan_count
    FROM payment_transactions
    GROUP BY ln_group
),

-- Count from base_late_payments (from monthly_late_fees.sql) for validation
validation_count AS (
    SELECT 
        CASE 
            WHEN li.LN = 0 THEN 'LN0'
            WHEN li.LN = 1 THEN 'LN1'
            WHEN li.LN = 2 THEN 'LN2'
            WHEN li.LN BETWEEN 3 AND 5 THEN 'LN3-5'
            WHEN li.LN BETWEEN 6 AND 8 THEN 'LN6-8'
            WHEN li.LN BETWEEN 9 AND 11 THEN 'LN9-11'
            ELSE 'LN12+'
        END as ln_group,
        COUNT(DISTINCT blp.client_id) as validation_client_count,
        COUNT(DISTINCT blp.loan_id) as validation_loan_count
    FROM base_late_payments blp
    JOIN ml.loan_info_tbl li ON blp.loan_key = li.loan_key
    WHERE payment_period = 'arrears_period'
    AND YEAR(blp.disbursementdate) >= 2024
    GROUP BY 1
)

-- Compare counts
SELECT 
    o.ln_group,
    o.original_client_count,
    v.validation_client_count,
    o.original_loan_count,
    v.validation_loan_count,
    o.original_client_count - v.validation_client_count as client_count_diff,
    o.original_loan_count - v.validation_loan_count as loan_count_diff
FROM original_count o
FULL OUTER JOIN validation_count v ON o.ln_group = v.ln_group
ORDER BY o.ln_group; 