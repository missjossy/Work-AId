WITH relevant_notifications AS (
    SELECT 
        timestamp,
        "TYPE",
        PARSE_JSON(PAYLOAD):user_id::string as user_id,
        PARSE_JSON(PAYLOAD):client_id::string as client_id,
        PARSE_JSON(PAYLOAD):status::string as status
    FROM data.backend_notifications
    WHERE ("TYPE" = 'BE_AUTHENTICATION_PHONE_VERIFICATION_2' OR "TYPE" = 'BE_LOAN_REPAYMENT_RESULT')
    AND TO_TIMESTAMP_NTZ(timestamp) >= '2024-01-01'
),

base_loans AS (
    SELECT 
        li.loan_key,
        li.loan_id,
        li.client_id,
        li.disbursementdate,
        li.penaltypaid,
        li.repaymentinstallments,
        rte.installment,
        rte.repayment_due_date,
        rte.transaction_date,
        rte.amount as payment_amount,
        rte.total_due,
        rte.penalty_amount,
        DATE_TRUNC('MONTH', rte.repayment_due_date) as due_month,
        DATEDIFF('day', rte.repayment_due_date, CURRENT_DATE) as days_late,
        FIRST_VALUE(rte.total_due) OVER (PARTITION BY li.loan_key, rte.installment ORDER BY rte.transaction_date) as installment_due_amount
    FROM ml.loan_info_tbl li
    LEFT JOIN ml.repayment_transactions_extended rte 
        ON li.loan_key = rte.loan_key
        AND rte.transaction_date > rte.repayment_due_date
    WHERE li.disbursementdate IS NOT NULL
        AND li.disbursementdate >= '2024-01-01'
        AND days_from_first_due_installment >= 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY li.loan_key, rte.installment ORDER BY rte.transaction_date DESC) = 1
),

base_late_payments AS (
    SELECT 
        bl.*,
        CASE 
            WHEN days_late <= 2 THEN 'tolerance_period'
            ELSE 'arrears_period'
        END as payment_period
    FROM base_loans bl
),

login_activity AS (
    SELECT DISTINCT
        bu.banking_platform_id as client_id,
        DATE(TO_TIMESTAMP_NTZ(rn.timestamp)) as activity_date
    FROM relevant_notifications rn
    JOIN banking_service.user bu ON rn.user_id = bu.id
    WHERE rn."TYPE" = 'BE_AUTHENTICATION_PHONE_VERIFICATION_2'
    AND EXISTS (
        SELECT 1 
        FROM base_late_payments blp 
        WHERE blp.client_id = bu.banking_platform_id
        AND DATE(blp.transaction_date) = DATE(TO_TIMESTAMP_NTZ(rn.timestamp))
    )
),

failed_payments AS (
    SELECT DISTINCT
        rn.client_id,
        DATE(TO_TIMESTAMP_NTZ(rn.timestamp)) as activity_date
    FROM relevant_notifications rn
    WHERE rn."TYPE" = 'BE_LOAN_REPAYMENT_RESULT'
    AND rn.status != 'succeeded'
    AND EXISTS (
        SELECT 1 
        FROM base_late_payments blp 
        WHERE blp.client_id = rn.client_id
        AND DATE(blp.transaction_date) = DATE(TO_TIMESTAMP_NTZ(rn.timestamp))
    )
),

combined_activity AS (
    SELECT 
        blp.*,
        CASE 
            WHEN payment_amount >= installment_due_amount THEN 'full_payment'
            WHEN payment_amount < installment_due_amount AND payment_amount > 0 THEN 'partial_payment'
            WHEN fp.client_id IS NOT NULL THEN 'failed_payment'
            WHEN la.client_id IS NOT NULL THEN 'login_only'
            ELSE 'no_activity'
        END as payment_activity
    FROM base_late_payments blp
    LEFT JOIN login_activity la 
        ON blp.client_id = la.client_id 
        AND DATE(blp.transaction_date) = la.activity_date
    LEFT JOIN failed_payments fp 
        ON blp.client_id = fp.client_id 
        AND DATE(blp.transaction_date) = fp.activity_date
)

SELECT 
    due_month,
    payment_period,
    payment_activity,
    COUNT(DISTINCT loan_id) as number_of_loans,
    COUNT(DISTINCT client_id) as number_of_clients,
    COUNT(DISTINCT installment) as number_of_installments,
    SUM(CASE WHEN payment_activity IN ('full_payment', 'partial_payment') THEN payment_amount ELSE 0 END) as total_payments,
    SUM(DISTINCT installment_due_amount) as total_amount_due,
    SUM(penalty_amount) as total_penalty_charged,
    SUM(penaltypaid) as total_penalty_paid,
    AVG(days_late) as avg_days_late,
    SUM(CASE WHEN days_late > 30 THEN 1 ELSE 0 END) as loans_over_30_days_late
FROM combined_activity
WHERE payment_period = 'arrears_period'  -- Focus only on arrears, not tolerance
GROUP BY 
    due_month,
    payment_period,
    payment_activity
ORDER BY 
    due_month,
    payment_activity; 