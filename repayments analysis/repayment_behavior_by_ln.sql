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
        rte.installment,
        rte.repayment_due_date,
        rte.transaction_date,
        rte.amount as payment_amount,
        rte.total_due,
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
),

loan_repayment_behavior AS (
    SELECT 
        SUBSTRING(loan_id, 1, 2) as loan_number,
        payment_activity,
        payment_period,
        COUNT(DISTINCT loan_id) as loan_count
    FROM combined_activity
    GROUP BY 1, 2, 3
),

loan_totals AS (
    SELECT 
        loan_number,
        SUM(loan_count) as total_loans
    FROM loan_repayment_behavior
    GROUP BY 1
)

SELECT 
    lrb.loan_number,
    lrb.payment_period,
    lrb.payment_activity,
    lrb.loan_count,
    ROUND(100.0 * lrb.loan_count / lt.total_loans, 2) as percentage_of_loans,
    lt.total_loans as total_loans_in_category
FROM loan_repayment_behavior lrb
JOIN loan_totals lt ON lrb.loan_number = lt.loan_number
ORDER BY 
    lrb.loan_number,
    lrb.payment_period,
    percentage_of_loans DESC; 