-- Loan-Savings Timing Examples and Patterns
-- Shows specific examples of withdrawal-repayment and withdrawal-loan patterns

WITH base_savings_data AS (
    SELECT DISTINCT
        cl.id as client_id, 
        sa.id as account_id,
        sa.creationdate as creation_date,
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
)

-- Question 1: Are users withdrawing to pay back loans?
-- Show withdrawals that happen close to loan repayments
SELECT 
    'WITHDRAWAL-REPAYMENT PATTERN' as pattern_type,
    st.client_id,
    st.transaction_date as withdrawal_date,
    st.amount as withdrawal_amount,
    ld.loan_id,
    lr.transaction_date as repayment_date,
    lr.amount as repayment_amount,
    ld.ln,
    DATEDIFF('day', st.transaction_date, lr.transaction_date) as days_withdrawal_to_repayment,
    CASE 
        WHEN DATEDIFF('day', st.transaction_date, lr.transaction_date) = 0 THEN 'Same Day'
        WHEN DATEDIFF('day', st.transaction_date, lr.transaction_date) BETWEEN 1 AND 3 THEN '1-3 Days Before'
        WHEN DATEDIFF('day', st.transaction_date, lr.transaction_date) BETWEEN 4 AND 7 THEN '4-7 Days Before'
        WHEN DATEDIFF('day', st.transaction_date, lr.transaction_date) BETWEEN 8 AND 14 THEN '1-2 Weeks Before'
        ELSE 'More than 2 Weeks'
    END as timing_category,
    -- Amount comparison
    CASE 
        WHEN st.amount >= lr.amount * 0.8 AND st.amount <= lr.amount * 1.2 THEN 'Similar Amounts'
        WHEN st.amount > lr.amount THEN 'Withdrawal > Repayment'
        ELSE 'Withdrawal < Repayment'
    END as amount_relationship
FROM savings_transactions st
JOIN loan_data ld ON st.client_id = ld.client_id
JOIN ml.repayment_transactions_extended lr ON ld.loan_id = lr.loan_id
WHERE st.transaction_type = 'WITHDRAWAL'
AND lr.transaction_date IS NOT NULL
AND DATEDIFF('day', st.transaction_date, lr.transaction_date) BETWEEN 0 AND 14  -- Focus on close timing

UNION ALL

-- Question 2: Are they taking loans right after withdrawal?
-- Show loans taken after withdrawals
SELECT 
    'WITHDRAWAL-LOAN PATTERN' as pattern_type,
    st2.client_id,
    st2.transaction_date as withdrawal_date,
    st2.amount as withdrawal_amount,
    ld2.loan_id,
    ld2.disbursementdate as loan_date,
    ld2.loan_amount,
    ld2.ln,
    DATEDIFF('day', st2.transaction_date, ld2.disbursementdate) as days_withdrawal_to_loan,
    CASE 
        WHEN DATEDIFF('day', st2.transaction_date, ld2.disbursementdate) = 0 THEN 'Same Day'
        WHEN DATEDIFF('day', st2.transaction_date, ld2.disbursementdate) BETWEEN 1 AND 3 THEN '1-3 Days After'
        WHEN DATEDIFF('day', st2.transaction_date, ld2.disbursementdate) BETWEEN 4 AND 7 THEN '4-7 Days After'
        WHEN DATEDIFF('day', st2.transaction_date, ld2.disbursementdate) BETWEEN 8 AND 14 THEN '1-2 Weeks After'
        ELSE 'More than 2 Weeks'
    END as timing_category,
    -- Amount comparison
    CASE 
        WHEN st2.amount >= ld2.loan_amount * 0.8 AND st2.amount <= ld2.loan_amount * 1.2 THEN 'Similar Amounts'
        WHEN st2.amount > ld2.loan_amount THEN 'Withdrawal > Loan'
        ELSE 'Withdrawal < Loan'
    END as amount_relationship
FROM savings_transactions st2
JOIN loan_data ld2 ON st2.client_id = ld2.client_id
WHERE st2.transaction_type = 'WITHDRAWAL'
AND DATEDIFF('day', st2.entrydate, ld2.disbursementdate) BETWEEN 0 AND 14  -- Focus on close timing
ORDER BY client_id, transaction_date;
