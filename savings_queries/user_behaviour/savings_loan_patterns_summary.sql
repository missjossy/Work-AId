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
    and sa.accountstate != 'WITHDRAWN'
    AND (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
    AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE2 is null) 
    AND date(sa.creationdate) >= date('2025-04-03')
    AND date(sa.creationdate) between '2025-04-03' and '2025-10-15'
),

-- Get all savings withdrawals
savings_withdrawals AS (
    SELECT 
        st.parentaccountkey as account_key,
        st.creationdate as withdrawal_date,
        st.amount as withdrawal_amount,
        bsd.client_id
    FROM GHANA_PROD.MAMBU.SAVINGSTRANSACTION st
    JOIN base_savings_data bsd ON st.parentaccountkey = bsd.account_key
    WHERE st."type" = 'WITHDRAWAL'
    AND date(st.creationdate) BETWEEN date('2025-04-03') and date('2025-10-15')
),

-- Get all loan disbursements
loan_disbursements AS (
    SELECT 
        client_id,
        loan_id,
        ln,
        disbursementdate,
        loanamount as loan_amount
    FROM ml.loan_info_tbl
    WHERE disbursementdate IS NOT NULL
    AND ACCOUNTSTATE != 'CLOSED_WRITTEN_OFF'
    AND date(disbursementdate) BETWEEN date('2025-04-03') and date('2025-10-15')
),

-- Get all loan repayments
loan_repayments AS (
    SELECT 
        rte.loan_id,
        transaction_date as repayment_date,
        amount as repayment_amount,
        li.client_id,
        li.ln
    FROM ml.repayment_transactions_extended rte
    JOIN ml.loan_info_tbl li ON rte.loan_id = li.loan_id
    WHERE date(transaction_date) BETWEEN date('2025-04-03') and date('2025-10-15')
),

-- Get most recent loan for each client
most_recent_loans AS (
    SELECT 
        client_id,
        loan_id,
        ln,
        disbursementdate,
        loanamount,
        accountstate,
        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY disbursementdate DESC) as loan_rank
    FROM ml.loan_info_tbl
    WHERE disbursementdate IS NOT NULL
    AND ACCOUNTSTATE != 'CLOSED_WRITTEN_OFF'
),

-- Get active loans at time of withdrawal (most recent loan only)
active_loans_during_withdrawal AS (
    SELECT 
        sw.client_id,
        sw.withdrawal_date,
        sw.withdrawal_amount,
        mrl.loan_id,
        mrl.ln,
        mrl.accountstate as loan_status_at_withdrawal,
        mrl.disbursementdate,
        mrl.loanamount,
        -- Get repayment info for this specific loan
        lr.repayment_date as last_repayment_date,
        lr.repayment_amount as last_repayment_amount,
        -- Calculate days from withdrawal to repayment
        DATEDIFF('day', sw.withdrawal_date, lr.repayment_date) as days_withdrawal_to_repayment
    FROM savings_withdrawals sw
    JOIN most_recent_loans mrl ON sw.client_id = mrl.client_id
    LEFT JOIN (
        SELECT 
            loan_id,
            transaction_date as repayment_date,
            amount as repayment_amount,
            ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY transaction_date DESC) as repayment_rank
        FROM ml.repayment_transactions_extended
    ) lr ON mrl.loan_id = lr.loan_id AND lr.repayment_rank = 1
    WHERE mrl.loan_rank = 1  -- Only most recent loan
    AND mrl.disbursementdate <= sw.withdrawal_date  -- Loan was disbursed before withdrawal
    AND (mrl.accountstate = 'ACTIVE' OR mrl.accountstate = 'ACTIVE_IN_ARREARS')  -- Loan was active at withdrawal time
),

-- Analyze customer behavior patterns holistically
customer_behavior_patterns AS (
    SELECT 
        bsd.client_id,
        -- Savings behavior
        COUNT(DISTINCT sw.account_key) as savings_accounts_with_withdrawals,
        COUNT(sw.withdrawal_date) as total_withdrawals,
        SUM(sw.withdrawal_amount) as total_withdrawal_amount,
        MIN(sw.withdrawal_date) as first_withdrawal_date,
        MAX(sw.withdrawal_date) as last_withdrawal_date,
        
        -- Loan behavior
        COUNT(DISTINCT ld.loan_id) as total_loans,
        SUM(ld.loan_amount) as total_loan_amount,
        MIN(ld.disbursementdate) as first_loan_date,
        MAX(ld.disbursementdate) as last_loan_date,
        MAX(ld.ln) as max_loan_number,
        
        -- Repayment behavior
        COUNT(DISTINCT lr.loan_id) as loans_with_repayments,
        COUNT(lr.repayment_date) as total_repayments,
        SUM(lr.repayment_amount) as total_repayment_amount,
        MIN(lr.repayment_date) as first_repayment_date,
        MAX(lr.repayment_date) as last_repayment_date,
        
        -- Active loan during withdrawal behavior
        COUNT(DISTINCT alw.loan_id) as active_loans_during_withdrawal,
        SUM(alw.withdrawal_amount) as withdrawal_amount_with_active_loans,
        MIN(alw.withdrawal_date) as first_withdrawal_with_active_loan,
        MAX(alw.withdrawal_date) as last_withdrawal_with_active_loan,
        AVG(alw.days_withdrawal_to_repayment) as avg_days_withdrawal_to_repayment,
        MAX(alw.last_repayment_date) as active_loan_last_repayment_date,
        MAX(alw.last_repayment_amount) as active_loan_last_repayment_amount,
        
        -- Timing analysis
        DATEDIFF('day', MIN(sw.withdrawal_date), MIN(ld.disbursementdate)) as days_first_withdrawal_to_first_loan,
        DATEDIFF('day', MIN(ld.disbursementdate), MIN(lr.repayment_date)) as days_first_loan_to_first_repayment,
        DATEDIFF('day', MIN(sw.withdrawal_date), MIN(lr.repayment_date)) as days_first_withdrawal_to_first_repayment,
        
        -- Behavior classification
        CASE 
            WHEN COUNT(sw.withdrawal_date) = 0 AND COUNT(ld.loan_id) = 0 THEN 'savings_only_no_withdrawals'
            WHEN COUNT(sw.withdrawal_date) > 0 AND COUNT(ld.loan_id) = 0 THEN 'savings_only_with_withdrawals'
            WHEN COUNT(sw.withdrawal_date) = 0 AND COUNT(ld.loan_id) > 0 THEN 'loans_only_no_savings_withdrawals'
            WHEN COUNT(sw.withdrawal_date) > 0 AND COUNT(ld.loan_id) > 0 THEN 'mixed_behavior'
            ELSE 'other'
        END as behavior_type,
        
        -- Withdrawal timing relative to loans
        CASE 
            WHEN COUNT(sw.withdrawal_date) > 0 AND COUNT(ld.loan_id) > 0 THEN
                CASE 
                    WHEN MIN(sw.withdrawal_date) <= MIN(ld.disbursementdate) THEN 'withdrawals_before_loans'
                    WHEN MIN(sw.withdrawal_date) > MIN(ld.disbursementdate) THEN 'withdrawals_after_loans'
                    ELSE 'mixed_timing'
                END
            ELSE 'no_loan_withdrawals'
        END as withdrawal_loan_timing,
        
        -- Withdrawal timing relative to repayments
        CASE 
            WHEN COUNT(sw.withdrawal_date) > 0 AND COUNT(lr.repayment_date) > 0 THEN
                CASE 
                    WHEN MIN(sw.withdrawal_date) <= MIN(lr.repayment_date) THEN 'withdrawals_before_repayments'
                    WHEN MIN(sw.withdrawal_date) > MIN(lr.repayment_date) THEN 'withdrawals_after_repayments'
                    ELSE 'mixed_timing'
                END
            ELSE 'no_repayment_withdrawals'
        END as withdrawal_repayment_timing
        
    FROM base_savings_data bsd
    LEFT JOIN savings_withdrawals sw ON bsd.client_id = sw.client_id
    LEFT JOIN loan_disbursements ld ON bsd.client_id = ld.client_id
    LEFT JOIN loan_repayments lr ON bsd.client_id = lr.client_id
    LEFT JOIN active_loans_during_withdrawal alw ON bsd.client_id = alw.client_id
    GROUP BY bsd.client_id
)

-- Analysis: Users who withdrew while with active loans
-- Breakdown by repayment timing after withdrawal
SELECT 
    'Repayment < 24 hours after withdrawal' as category,
    COUNT(*) as client_count,
    ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER(), 0), 2) as percentage,
    SUM(withdrawal_amount_with_active_loans) as total_withdrawal_amount,
    SUM(total_loan_amount) as total_loan_amount,
    SUM(active_loan_last_repayment_amount) as total_repayment_amount,
    AVG(avg_days_withdrawal_to_repayment) as avg_days_to_repayment
FROM customer_behavior_patterns
WHERE active_loans_during_withdrawal > 0
AND avg_days_withdrawal_to_repayment <= 1

UNION ALL

SELECT 
    'Later repayment' as category,
    COUNT(*) as client_count,
    ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER(), 0), 2) as percentage,
    SUM(withdrawal_amount_with_active_loans) as total_withdrawal_amount,
    SUM(total_loan_amount) as total_loan_amount,
    SUM(active_loan_last_repayment_amount) as total_repayment_amount,
    AVG(avg_days_withdrawal_to_repayment) as avg_days_to_repayment
FROM customer_behavior_patterns
WHERE active_loans_during_withdrawal > 0
AND avg_days_withdrawal_to_repayment > 1
AND active_loan_last_repayment_date IS NOT NULL

UNION ALL

SELECT 
    'Still owing (for that loan)' as category,
    COUNT(*) as client_count,
    ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER(), 0), 2) as percentage,
    SUM(withdrawal_amount_with_active_loans) as total_withdrawal_amount,
    SUM(total_loan_amount) as total_loan_amount,
    SUM(active_loan_last_repayment_amount) as total_repayment_amount,
    NULL as avg_days_to_repayment
FROM customer_behavior_patterns
WHERE active_loans_during_withdrawal > 0
AND active_loan_last_repayment_date IS NULL

ORDER BY 
    CASE category
        WHEN 'Repayment < 24 hours after withdrawal' THEN 1
        WHEN 'Later repayment' THEN 2
        WHEN 'Still owing (for that loan)' THEN 3
    END;
