WITH late_installments AS (
    -- Pre-filter late installments to reduce data volume early
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
        rte.repayment_state,
        DATEDIFF('day', rte.repayment_due_date, CURRENT_DATE) as days_late
    FROM ml.loan_info_tbl li
    JOIN (
        -- Subquery to get latest transaction per installment
        SELECT loan_key, installment, MAX(transaction_date) as max_transaction_date
        FROM ml.repayment_transactions_extended
        WHERE repayment_due_date < CURRENT_DATE
        GROUP BY loan_key, installment
    ) latest ON li.loan_key = latest.loan_key
    JOIN ml.repayment_transactions_extended rte 
        ON latest.loan_key = rte.loan_key
        AND latest.installment = rte.installment
        AND latest.max_transaction_date = rte.transaction_date
    WHERE li.disbursementdate >= '2024-01-01'
        AND days_from_first_due_installment >= 0
),

failed_payments AS (
    -- Pre-aggregate failed payments to reduce join complexity
    SELECT 
        client_id,
        activity_date,
        phone_number,
        failure_state,
        failure_reason,
        attempted_amount
    FROM (
        SELECT 
            PARSE_JSON(payment_details):banking_platform_id::integer as client_id,
            DATE(tm.created_on) as activity_date,
            PARSE_JSON(payment_details):walletId::integer as phone_number,
            ts.STATE as failure_state,
            ts.description as failure_reason,
            amount as attempted_amount,
            ROW_NUMBER() OVER (
                PARTITION BY PARSE_JSON(payment_details):banking_platform_id::integer, DATE(tm.created_on)
                ORDER BY tm.created_on DESC
            ) as rn
        FROM money_transfer.transaction_metadata_p tm
        JOIN money_transfer.transaction_states_p ts ON tm.ID = ts.ID
        WHERE transaction_type = 'DEPOSIT'
        AND PARSE_JSON(PAYMENT_DETAILS):paymentType::string in ('MAIN', 'BUSINESS')
        AND ts.SUCCESS = 'FALSE'
        AND DATE(tm.created_on) >= '2024-01-01'
    )
    WHERE rn = 1
),

login_activity AS (
    -- Pre-aggregate login activity
    SELECT DISTINCT
        bu.banking_platform_id as client_id,
        DATE(bn.timestamp) as activity_date
    FROM data.backend_notifications bn
    JOIN banking_service.user bu ON bn.user_identity = bu.id
    WHERE bn."TYPE" = 'BE_AUTHENTICATION_PHONE_VERIFICATION_2'
    AND TO_TIMESTAMP_NTZ(bn.timestamp) >= '2024-01-01'
),

combined_activity AS (
    SELECT 
        li.*,
        CASE 
            WHEN li.repayment_state = 'PAID' THEN 'full_payment'
            WHEN li.repayment_state = 'PARTIALLY_PAID' THEN 'partial_payment'
            WHEN fp.client_id IS NOT NULL 
                AND DATEDIFF('day', li.repayment_due_date, fp.activity_date) BETWEEN 0 AND 2
                THEN 'failed_payment_tolerance'
            WHEN fp.client_id IS NOT NULL 
                AND DATEDIFF('day', li.repayment_due_date, fp.activity_date) > 2
                THEN 'failed_payment_arrears'
            WHEN la.client_id IS NOT NULL 
                AND DATEDIFF('day', li.repayment_due_date, la.activity_date) BETWEEN 0 AND 2
                THEN 'login_only_tolerance'
            WHEN la.client_id IS NOT NULL 
                AND DATEDIFF('day', li.repayment_due_date, la.activity_date) > 2
                THEN 'login_only_arrears'
            ELSE 'no_activity'
        END as payment_activity,
        CASE 
            WHEN li.repayment_state IN ('PAID', 'PARTIALLY_PAID') THEN
                CASE 
                    WHEN DATEDIFF('day', li.repayment_due_date, li.transaction_date) BETWEEN 0 AND 2 THEN 'tolerance_period'
                    ELSE 'arrears_period'
                END
            WHEN fp.client_id IS NOT NULL THEN
                CASE 
                    WHEN DATEDIFF('day', li.repayment_due_date, fp.activity_date) BETWEEN 0 AND 2 THEN 'tolerance_period'
                    ELSE 'arrears_period'
                END
            WHEN la.client_id IS NOT NULL THEN
                CASE 
                    WHEN DATEDIFF('day', li.repayment_due_date, la.activity_date) BETWEEN 0 AND 2 THEN 'tolerance_period'
                    ELSE 'arrears_period'
                END
            ELSE 'arrears_period'
        END as payment_period,
        fp.failure_state,
        fp.failure_reason,
        fp.attempted_amount,
        fp.phone_number as failed_payment_phone,
        CASE 
            WHEN li.repayment_state IN ('PAID', 'PARTIALLY_PAID') THEN DATEDIFF('day', li.repayment_due_date, li.transaction_date)
            WHEN fp.client_id IS NOT NULL THEN DATEDIFF('day', li.repayment_due_date, fp.activity_date)
            WHEN la.client_id IS NOT NULL THEN DATEDIFF('day', li.repayment_due_date, la.activity_date)
            ELSE li.days_late
        END as activity_days_late
    FROM late_installments li
    LEFT JOIN login_activity la 
        ON li.client_id = la.client_id 
    LEFT JOIN failed_payments fp 
        ON li.client_id = fp.client_id 
)

SELECT 
    loan_id,
    client_id,
    disbursementdate,
    payment_period,
    payment_activity,
    failure_state,
    failure_reason,
    attempted_amount,
    failed_payment_phone,
    activity_days_late as last_payment_days_late
FROM combined_activity