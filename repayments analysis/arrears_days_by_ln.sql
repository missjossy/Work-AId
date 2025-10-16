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

combined_activity AS (
    SELECT 
        blp.*,
        CASE 
            WHEN payment_amount >= installment_due_amount THEN 'full_payment'
            WHEN payment_amount < installment_due_amount AND payment_amount > 0 THEN 'partial_payment'
            ELSE 'no_payment'
        END as payment_activity
    FROM base_late_payments blp
    WHERE payment_period = 'arrears_period'
),

loan_arrears_data AS (
    SELECT 
        SUBSTRING(loan_id, 1, 2) as loan_number,
        days_late,
        payment_activity,
        COUNT(DISTINCT loan_id) as loan_count
    FROM combined_activity
    WHERE payment_activity IN ('full_payment', 'partial_payment')
    GROUP BY 1, 2, 3
)

SELECT 
    loan_number,
    COUNT(DISTINCT CASE WHEN days_late <= 7 THEN loan_count END) as paid_within_week,
    COUNT(DISTINCT CASE WHEN days_late BETWEEN 8 AND 14 THEN loan_count END) as paid_within_two_weeks,
    COUNT(DISTINCT CASE WHEN days_late BETWEEN 15 AND 30 THEN loan_count END) as paid_within_month,
    COUNT(DISTINCT CASE WHEN days_late > 30 THEN loan_count END) as paid_after_month,
    ROUND(AVG(days_late), 1) as avg_days_to_payment,
    MIN(days_late) as min_days_to_payment,
    MAX(days_late) as max_days_to_payment,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_late) as median_days_to_payment,
    COUNT(DISTINCT loan_count) as total_loans_paid
FROM loan_arrears_data
GROUP BY loan_number
ORDER BY loan_number; 