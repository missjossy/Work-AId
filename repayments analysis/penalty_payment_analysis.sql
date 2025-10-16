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

late_loan_clients AS (
    SELECT DISTINCT
        client_id,
        SUM(penaltypaid) as total_penalties_paid
    FROM base_late_payments
    GROUP BY client_id
)

SELECT 
    COUNT(DISTINCT client_id) as total_late_loan_clients,
    COUNT(DISTINCT CASE WHEN total_penalties_paid > 0 THEN client_id END) as clients_with_penalties,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN total_penalties_paid > 0 THEN client_id END) / 
          NULLIF(COUNT(DISTINCT client_id), 0), 2) as percentage_paying_penalties
FROM late_loan_clients; 