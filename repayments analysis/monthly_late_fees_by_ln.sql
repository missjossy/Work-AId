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
        DATE_TRUNC('month', rte.transaction_date) as payment_month,
        ROW_NUMBER() OVER (PARTITION BY rte.loan_key, rte.installment ORDER BY rte.transaction_date DESC) as payment_rank,
        FIRST_VALUE(rte.total_due) OVER (PARTITION BY rte.loan_key, rte.installment ORDER BY rte.transaction_date) as installment_due_amount
    FROM ml.repayment_transactions_extended rte
    JOIN ml.loan_info_tbl li ON rte.loan_key = li.loan_key
    WHERE 
        rte.disbursementdate IS NOT NULL
        AND YEAR(rte.disbursementdate) >= 2024
        AND rte.transaction_date > rte.repayment_due_date
),

login_activity AS (
    SELECT 
        client_id,
        DATE_TRUNC('month', created_at) as activity_month,
        'login' as activity_type
    FROM data.backend_notifications
    WHERE "TYPE" = 'BE_AUTHENTICATION_PHONE_VERIFICATION_2'
    AND IDENTITY_TYPE = 'USER_ID'
),

failed_payments AS (
    SELECT 
        PARSE_JSON(PAYLOAD):client_id::string as client_id,
        DATE_TRUNC('month', created_at) as activity_month,
        'failed_payment' as activity_type
    FROM data.backend_notifications
    WHERE "TYPE" = 'BE_LOAN_REPAYMENT_RESULT'
    AND PARSE_JSON(PAYLOAD):status::string != 'succeeded'
),

combined_activity AS (
    SELECT 
        pt.*,
        CASE 
            WHEN payment_amount >= installment_due_amount THEN 'full_payment'
            WHEN payment_amount < installment_due_amount THEN 'partial_payment'
            WHEN fp.client_id IS NOT NULL THEN 'failed_payment'
            WHEN la.client_id IS NOT NULL THEN 'login_only'
            ELSE 'no_activity'
        END as payment_activity
    FROM payment_transactions pt
    LEFT JOIN login_activity la 
        ON pt.client_id = la.client_id 
        AND pt.payment_month = la.activity_month
    LEFT JOIN failed_payments fp 
        ON pt.client_id = fp.client_id 
        AND pt.payment_month = fp.activity_month
    WHERE payment_rank = 1  -- Only consider the last payment for each installment
),

monthly_activity AS (
    SELECT 
        payment_month,
        ln_group,
        loan_type,
        payment_activity,
        COUNT(DISTINCT client_id) as number_of_clients,
        COUNT(DISTINCT loan_id) as number_of_loans,
        COUNT(DISTINCT installment) as number_of_late_installments,
        SUM(CASE WHEN payment_activity IN ('full_payment', 'partial_payment') THEN 1 ELSE 0 END) as installments_with_payments,
        COUNT(DISTINCT CASE WHEN penaltypaid > 0 THEN client_id END) as clients_with_penalties,
        SUM(CASE WHEN payment_activity IN ('full_payment', 'partial_payment') THEN payment_amount ELSE 0 END) as total_payments,
        SUM(penaltypaid) as total_penalties_paid,
        AVG(CASE WHEN penaltypaid > 0 THEN penaltypaid END) as avg_penalty_per_installment
    FROM combined_activity
    GROUP BY 
        payment_month,
        ln_group,
        loan_type,
        payment_activity
)

SELECT 
    payment_month,
    ln_group,
    loan_type,
    payment_activity,
    number_of_clients,
    number_of_loans,
    number_of_late_installments,
    installments_with_payments,
    clients_with_penalties,
    total_payments,
    total_penalties_paid,
    avg_penalty_per_installment,
    ROUND(100.0 * clients_with_penalties / NULLIF(number_of_clients, 0), 2) as pct_clients_with_penalties,
    ROUND(100.0 * installments_with_payments / NULLIF(number_of_late_installments, 0), 2) as pct_installments_with_payments
FROM monthly_activity
ORDER BY 
    payment_month,
    ln_group,
    loan_type,
    payment_activity; 