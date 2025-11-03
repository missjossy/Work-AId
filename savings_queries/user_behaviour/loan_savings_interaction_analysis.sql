-- Loan-Savings Interaction Analysis
-- Answers: Are users withdrawing to pay back loans? Are they taking loans after withdrawal?

WITH base_savings_data AS (
    SELECT DISTINCT
        cl.id as client_id, 
        sa.id as account_id,
        sa.creationdate as creation_date,
        sa.closeddate,
        sa.accountstate,
        sa.encodedkey as account_key,
        cl.birthdate,
        DATEDIFF('year', cl.birthdate, current_date) as age
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
        st.balance,
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
        li.last_expected_repayment,
        li.loanamount as loan_amount,
        li.ln,
        li.accountstate as loan_status,
        -- Get repayment dates
        lr.repayment_date,
        lr.amount as repayment_amount
    FROM ml.loan_info_tbl li
    LEFT JOIN (
        SELECT 
            loan_id,
            transaction_date as repayment_date,
            amount
        FROM ml.repayment_transactions_extended 
        WHERE transaction_date IS NOT NULL
    ) lr ON li.loan_id = lr.loan_id
    WHERE li.disbursementdate IS NOT NULL
),

-- Find withdrawals that happen close to loan repayments
withdrawals_near_repayments AS (
    SELECT 
        st.client_id,
        st.transaction_date as withdrawal_date,
        st.amount as withdrawal_amount,
        ld.loan_id,
        ld.repayment_date,
        ld.repayment_amount,
        ld.ln,
        DATEDIFF('day', st.transaction_date, ld.repayment_date) as days_withdrawal_to_repayment,
        CASE 
            WHEN DATEDIFF('day', st.transaction_date, ld.repayment_date) BETWEEN 0 AND 7 THEN 'Same Week'
            WHEN DATEDIFF('day', st.transaction_date, ld.repayment_date) BETWEEN 8 AND 14 THEN '1-2 Weeks Before'
            WHEN DATEDIFF('day', st.transaction_date, ld.repayment_date) BETWEEN 15 AND 30 THEN '2-4 Weeks Before'
            ELSE 'More than 1 Month'
        END as timing_category
    FROM savings_transactions st
    JOIN loan_data ld ON st.client_id = ld.client_id
    WHERE st.transaction_type = 'WITHDRAWAL'
    AND ld.repayment_date IS NOT NULL
    AND DATEDIFF('day', st.transaction_date, ld.repayment_date) BETWEEN 0 AND 30
),

-- Find loans taken after withdrawals
loans_after_withdrawals AS (
    SELECT 
        st.client_id,
        st.transaction_date as withdrawal_date,
        st.amount as withdrawal_amount,
        ld.loan_id,
        ld.disbursementdate as loan_date,
        ld.loan_amount,
        ld.ln,
        DATEDIFF('day', st.transaction_date, ld.disbursementdate) as days_withdrawal_to_loan,
        CASE 
            WHEN DATEDIFF('day', st.transaction_date, ld.disbursementdate) BETWEEN 0 AND 7 THEN 'Same Week'
            WHEN DATEDIFF('day', st.transaction_date, ld.disbursementdate) BETWEEN 8 AND 14 THEN '1-2 Weeks After'
            WHEN DATEDIFF('day', st.transaction_date, ld.disbursementdate) BETWEEN 15 AND 30 THEN '2-4 Weeks After'
            ELSE 'More than 1 Month'
        END as timing_category
    FROM savings_transactions st
    JOIN loan_data ld ON st.client_id = ld.client_id
    WHERE st.transaction_type = 'WITHDRAWAL'
    AND ld.disbursementdate IS NOT NULL
    AND DATEDIFF('day', st.transaction_date, ld.disbursementdate) BETWEEN 0 AND 30
),

-- Client-level summary
client_interaction_summary AS (
    SELECT 
        bsd.client_id,
        COUNT(DISTINCT wnr.withdrawal_date) as withdrawals_near_repayments,
        COUNT(DISTINCT law.withdrawal_date) as withdrawals_followed_by_loans,
        SUM(wnr.withdrawal_amount) as total_withdrawal_amount_near_repayments,
        SUM(law.loan_amount) as total_loan_amount_after_withdrawals,
        -- Average timing
        AVG(wnr.days_withdrawal_to_repayment) as avg_days_withdrawal_to_repayment,
        AVG(law.days_withdrawal_to_loan) as avg_days_withdrawal_to_loan
    FROM base_savings_data bsd
    LEFT JOIN withdrawals_near_repayments wnr ON bsd.client_id = wnr.client_id
    LEFT JOIN loans_after_withdrawals law ON bsd.client_id = law.client_id
    GROUP BY bsd.client_id
)

-- Final analysis
SELECT 
    'Withdrawals Near Repayments' as analysis_type,
    COUNT(DISTINCT client_id) as clients_with_pattern,
    COUNT(*) as total_instances,
    SUM(withdrawal_amount) as total_withdrawal_amount,
    SUM(repayment_amount) as total_repayment_amount,
    AVG(withdrawal_amount) as avg_withdrawal_amount,
    AVG(repayment_amount) as avg_repayment_amount,
    AVG(days_withdrawal_to_repayment) as avg_days_between,
    -- Timing breakdown
    COUNT(CASE WHEN timing_category = 'Same Week' THEN 1 END) as same_week_count,
    COUNT(CASE WHEN timing_category = '1-2 Weeks Before' THEN 1 END) as one_two_weeks_count,
    COUNT(CASE WHEN timing_category = '2-4 Weeks Before' THEN 1 END) as two_four_weeks_count,
    COUNT(CASE WHEN timing_category = 'More than 1 Month' THEN 1 END) as more_than_month_count
FROM withdrawals_near_repayments

UNION ALL

SELECT 
    'Loans After Withdrawals' as analysis_type,
    COUNT(DISTINCT client_id) as clients_with_pattern,
    COUNT(*) as total_instances,
    SUM(withdrawal_amount) as total_withdrawal_amount,
    SUM(loan_amount) as total_loan_amount,
    AVG(withdrawal_amount) as avg_withdrawal_amount,
    AVG(loan_amount) as avg_loan_amount,
    AVG(days_withdrawal_to_loan) as avg_days_between,
    -- Timing breakdown
    COUNT(CASE WHEN timing_category = 'Same Week' THEN 1 END) as same_week_count,
    COUNT(CASE WHEN timing_category = '1-2 Weeks After' THEN 1 END) as one_two_weeks_count,
    COUNT(CASE WHEN timing_category = '2-4 Weeks After' THEN 1 END) as two_four_weeks_count,
    COUNT(CASE WHEN timing_category = 'More than 1 Month' THEN 1 END) as more_than_month_count
FROM loans_after_withdrawals

ORDER BY analysis_type;
