-- Detailed Loan-Savings Interaction Patterns
-- Shows specific client behaviors and patterns

WITH base_savings_data AS (
    SELECT DISTINCT
        cl.id as client_id, 
        sa.id as account_id,
        sa.creationdate as creation_date,
        sa.closeddate,
        sa.accountstate,
        sa.encodedkey as account_key
    FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa
    LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
    WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
    AND (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
    AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE2 is null) 
    AND date(sa.creationdate) >= date('2025-04-03')
),

savings_transactions AS (
    SELECT 
        st.parentaccountkey as account_key,
        st.creationdate as transaction_date,
        st.amount,
        st."type" as transaction_type,
        bsd.client_id
    FROM GHANA_PROD.MAMBU.SAVINGSTRANSACTION st
    JOIN base_savings_data bsd ON st.parentaccountkey = bsd.account_key
    WHERE st."type" IN ('WITHDRAWAL', 'DEPOSIT')
    AND date(st.creationdate) BETWEEN date('2025-04-03') and date('2025-10-15')
),

loan_data AS (
    SELECT 
        li.client_id,
        li.loan_id,
        li.disbursementdate,
        li.loanamount as loan_amount,
        li.ln,
        li.accountstate as loan_status
    FROM ml.loan_info_tbl li
    WHERE li.disbursementdate IS NOT NULL
),

-- Pattern 1: Withdrawals close to loan repayments
withdrawal_repayment_patterns AS (
    SELECT 
        st.client_id,
        st.transaction_date as withdrawal_date,
        st.amount as withdrawal_amount,
        ld.loan_id,
        lr.transaction_date as repayment_date,
        lr.amount as repayment_amount,
        ld.ln,
        DATEDIFF('day', st.transaction_date, lr.transaction_date) as days_between,
        CASE 
            WHEN DATEDIFF('day', st.transaction_date, lr.transaction_date) BETWEEN 0 AND 3 THEN 'Same Day to 3 Days'
            WHEN DATEDIFF('day', st.transaction_date, lr.transaction_date) BETWEEN 4 AND 7 THEN '4-7 Days'
            WHEN DATEDIFF('day', st.transaction_date, lr.transaction_date) BETWEEN 8 AND 14 THEN '1-2 Weeks'
            WHEN DATEDIFF('day', st.transaction_date, lr.transaction_date) BETWEEN 15 AND 30 THEN '2-4 Weeks'
            ELSE 'More than 1 Month'
        END as timing_category
    FROM savings_transactions st
    JOIN loan_data ld ON st.client_id = ld.client_id
    JOIN ml.repayment_transactions_extended lr ON ld.loan_id = lr.loan_id
    WHERE st.transaction_type = 'WITHDRAWAL'
    AND lr.transaction_date IS NOT NULL
    AND DATEDIFF('day', st.transaction_date, lr.transaction_date) BETWEEN 0 AND 30
),

-- Pattern 2: Loans taken after withdrawals
withdrawal_loan_patterns AS (
    SELECT 
        st.client_id,
        st.transaction_date as withdrawal_date,
        st.amount as withdrawal_amount,
        ld.loan_id,
        ld.disbursementdate as loan_date,
        ld.loan_amount,
        ld.ln,
        DATEDIFF('day', st.transaction_date, ld.disbursementdate) as days_between,
        CASE 
            WHEN DATEDIFF('day', st.transaction_date, ld.disbursementdate) BETWEEN 0 AND 3 THEN 'Same Day to 3 Days'
            WHEN DATEDIFF('day', st.transaction_date, ld.disbursementdate) BETWEEN 4 AND 7 THEN '4-7 Days'
            WHEN DATEDIFF('day', st.transaction_date, ld.disbursementdate) BETWEEN 8 AND 14 THEN '1-2 Weeks'
            WHEN DATEDIFF('day', st.transaction_date, ld.disbursementdate) BETWEEN 15 AND 30 THEN '2-4 Weeks'
            ELSE 'More than 1 Month'
        END as timing_category
    FROM savings_transactions st
    JOIN loan_data ld ON st.client_id = ld.client_id
    WHERE st.transaction_type = 'WITHDRAWAL'
    AND DATEDIFF('day', st.transaction_date, ld.disbursementdate) BETWEEN 0 AND 30
),

-- Client behavior classification
client_behavior_classification AS (
    SELECT 
        bsd.client_id,
        -- Withdrawal-repayment patterns
        COUNT(DISTINCT wrp.withdrawal_date) as withdrawal_repayment_instances,
        SUM(wrp.withdrawal_amount) as total_withdrawal_for_repayments,
        AVG(wrp.days_between) as avg_days_withdrawal_to_repayment,
        -- Withdrawal-loan patterns  
        COUNT(DISTINCT wlp.withdrawal_date) as withdrawal_loan_instances,
        SUM(wlp.loan_amount) as total_loans_after_withdrawals,
        AVG(wlp.days_between) as avg_days_withdrawal_to_loan,
        -- Classification
        CASE 
            WHEN COUNT(DISTINCT wrp.withdrawal_date) > 0 AND COUNT(DISTINCT wlp.withdrawal_date) > 0 THEN 'Both Patterns'
            WHEN COUNT(DISTINCT wrp.withdrawal_date) > 0 THEN 'Withdrawal-Repayment Only'
            WHEN COUNT(DISTINCT wlp.withdrawal_date) > 0 THEN 'Withdrawal-Loan Only'
            ELSE 'No Clear Pattern'
        END as behavior_type
    FROM base_savings_data bsd
    LEFT JOIN withdrawal_repayment_patterns wrp ON bsd.client_id = wrp.client_id
    LEFT JOIN withdrawal_loan_patterns wlp ON bsd.client_id = wlp.client_id
    GROUP BY bsd.client_id
)

-- Summary by behavior type
SELECT 
    behavior_type,
    COUNT(DISTINCT client_id) as client_count,
    ROUND(COUNT(DISTINCT client_id) * 100.0 / SUM(COUNT(DISTINCT client_id)) OVER(), 2) as percentage_of_clients,
    -- Withdrawal-repayment metrics
    AVG(withdrawal_repayment_instances) as avg_withdrawal_repayment_instances,
    AVG(total_withdrawal_for_repayments) as avg_withdrawal_amount_for_repayments,
    AVG(avg_days_withdrawal_to_repayment) as avg_days_withdrawal_to_repayment,
    -- Withdrawal-loan metrics
    AVG(withdrawal_loan_instances) as avg_withdrawal_loan_instances,
    AVG(total_loans_after_withdrawals) as avg_loan_amount_after_withdrawals,
    AVG(avg_days_withdrawal_to_loan) as avg_days_withdrawal_to_loan
FROM client_behavior_classification
GROUP BY behavior_type
ORDER BY client_count DESC;
