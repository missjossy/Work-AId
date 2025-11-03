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
    AND date(sa.creationdate) between '2025-04-03' and '2025-10-15'
),

-- Get all savings withdrawals
savings_withdrawals AS (
    SELECT 
        st.parentaccountkey as account_key,
        st.creationdate as withdrawal_date,
        st.amount as withdrawal_amount,
        st.balance as balance_after_withdrawal,
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
        principal as loan_amount,
        accountstate
    FROM ml.loan_info_tbl
    WHERE disbursementdate IS NOT NULL
    AND ACCOUNTSTATE != 'CLOSED_WRITTEN_OFF'
    AND date(disbursementdate) BETWEEN date('2025-04-03') and date('2025-10-15')
),

-- Get all loan repayments
loan_repayments AS (
    SELECT 
        loan_id,
        transaction_date as repayment_date,
        amount as repayment_amount,
        repayment_state,
        li.client_id,
        li.ln
    FROM ml.repayment_transactions_extended rte
    JOIN ml.loan_info_tbl li ON rte.loan_id = li.loan_id
    WHERE date(transaction_date) BETWEEN date('2025-04-03') and date('2025-10-15')
),

-- Analyze withdrawals before loan disbursements
withdrawals_before_loans AS (
    SELECT 
        sw.client_id,
        sw.withdrawal_date,
        sw.withdrawal_amount,
        ld.loan_id as disbursement_loan_id,
        ld.disbursementdate,
        ld.loan_amount,
        DATEDIFF('day', sw.withdrawal_date, ld.disbursementdate) as days_withdrawal_to_loan,
        CASE 
            WHEN DATEDIFF('day', sw.withdrawal_date, ld.disbursementdate) BETWEEN 0 AND 7 THEN 'within_1_week'
            WHEN DATEDIFF('day', sw.withdrawal_date, ld.disbursementdate) BETWEEN 8 AND 30 THEN 'within_1_month'
            WHEN DATEDIFF('day', sw.withdrawal_date, ld.disbursementdate) > 30 THEN 'more_than_1_month'
            ELSE 'future_withdrawal'
        END as timing_category
    FROM savings_withdrawals sw
    JOIN loan_disbursements ld ON sw.client_id = ld.client_id
    WHERE sw.withdrawal_date <= ld.disbursementdate
),

-- Analyze withdrawals before loan repayments
withdrawals_before_repayments AS (
    SELECT 
        sw.client_id,
        sw.withdrawal_date,
        sw.withdrawal_amount,
        lr.loan_id as repayment_loan_id,
        lr.repayment_date,
        lr.repayment_amount,
        lr.ln,
        DATEDIFF('day', sw.withdrawal_date, lr.repayment_date) as days_withdrawal_to_repayment,
        CASE 
            WHEN DATEDIFF('day', sw.withdrawal_date, lr.repayment_date) BETWEEN 0 AND 3 THEN 'within_3_days'
            WHEN DATEDIFF('day', sw.withdrawal_date, lr.repayment_date) BETWEEN 4 AND 7 THEN 'within_1_week'
            WHEN DATEDIFF('day', sw.withdrawal_date, lr.repayment_date) BETWEEN 8 AND 30 THEN 'within_1_month'
            WHEN DATEDIFF('day', sw.withdrawal_date, lr.repayment_date) > 30 THEN 'more_than_1_month'
            ELSE 'future_withdrawal'
        END as timing_category
    FROM savings_withdrawals sw
    JOIN loan_repayments lr ON sw.client_id = lr.client_id
    WHERE sw.withdrawal_date <= lr.repayment_date
),

-- Client-level summary
client_interaction_summary AS (
    SELECT 
        bsd.client_id,
        -- Withdrawal metrics
        COUNT(DISTINCT sw.withdrawal_date) as total_withdrawal_days,
        SUM(sw.withdrawal_amount) as total_withdrawals,
        AVG(sw.withdrawal_amount) as avg_withdrawal_amount,
        -- Loan metrics
        COUNT(DISTINCT ld.loan_id) as total_loans,
        SUM(ld.loan_amount) as total_loan_amount,
        -- Interaction metrics
        COUNT(DISTINCT wbl.disbursement_loan_id) as loans_after_withdrawals,
        COUNT(DISTINCT wbr.repayment_loan_id) as repayments_after_withdrawals,
        -- Timing analysis
        COUNT(CASE WHEN wbl.timing_category = 'within_1_week' THEN 1 END) as loans_within_week_of_withdrawal,
        COUNT(CASE WHEN wbl.timing_category = 'within_1_month' THEN 1 END) as loans_within_month_of_withdrawal,
        COUNT(CASE WHEN wbr.timing_category = 'within_3_days' THEN 1 END) as repayments_within_3_days_of_withdrawal,
        COUNT(CASE WHEN wbr.timing_category = 'within_1_week' THEN 1 END) as repayments_within_week_of_withdrawal,
        -- Financial patterns
        AVG(wbl.days_withdrawal_to_loan) as avg_days_withdrawal_to_loan,
        AVG(wbr.days_withdrawal_to_repayment) as avg_days_withdrawal_to_repayment,
        -- Correlation analysis
        CASE 
            WHEN COUNT(DISTINCT wbl.loan_id) > 0 AND COUNT(DISTINCT sw.withdrawal_date) > 0 
            THEN COUNT(DISTINCT wbl.loan_id)::FLOAT / COUNT(DISTINCT sw.withdrawal_date)::FLOAT
            ELSE 0 
        END as loan_withdrawal_ratio,
        CASE 
            WHEN COUNT(DISTINCT wbr.loan_id) > 0 AND COUNT(DISTINCT sw.withdrawal_date) > 0 
            THEN COUNT(DISTINCT wbr.loan_id)::FLOAT / COUNT(DISTINCT sw.withdrawal_date)::FLOAT
            ELSE 0 
        END as repayment_withdrawal_ratio
    FROM base_savings_data bsd
    LEFT JOIN savings_withdrawals sw ON bsd.client_id = sw.client_id
    LEFT JOIN loan_disbursements ld ON bsd.client_id = ld.client_id
    LEFT JOIN withdrawals_before_loans wbl ON bsd.client_id = wbl.client_id
    LEFT JOIN withdrawals_before_repayments wbr ON bsd.client_id = wbr.client_id
    GROUP BY bsd.client_id
)

-- Final analysis
SELECT 
    client_id,
    total_withdrawal_days,
    total_withdrawals,
    avg_withdrawal_amount,
    total_loans,
    total_loan_amount,
    loans_after_withdrawals,
    repayments_after_withdrawals,
    loans_within_week_of_withdrawal,
    loans_within_month_of_withdrawal,
    repayments_within_3_days_of_withdrawal,
    repayments_within_week_of_withdrawal,
    avg_days_withdrawal_to_loan,
    avg_days_withdrawal_to_repayment,
    loan_withdrawal_ratio,
    repayment_withdrawal_ratio,
    -- Behavioral patterns
    CASE 
        WHEN loans_within_week_of_withdrawal > 0 THEN 'withdraws_then_borrows_quickly'
        WHEN loans_within_month_of_withdrawal > 0 THEN 'withdraws_then_borrows_moderately'
        WHEN repayments_within_3_days_of_withdrawal > 0 THEN 'withdraws_to_repay_immediately'
        WHEN repayments_within_week_of_withdrawal > 0 THEN 'withdraws_to_repay_quickly'
        WHEN total_withdrawals > 0 AND total_loans = 0 THEN 'withdraws_only'
        WHEN total_loans > 0 AND total_withdrawals = 0 THEN 'borrows_only'
        WHEN total_withdrawals > 0 AND total_loans > 0 THEN 'mixed_behavior'
        ELSE 'no_activity'
    END as interaction_pattern,
    -- Risk assessment
    CASE 
        WHEN loan_withdrawal_ratio > 0.5 AND avg_days_withdrawal_to_loan < 7 THEN 'high_risk_quick_borrowing'
        WHEN repayment_withdrawal_ratio > 0.5 AND avg_days_withdrawal_to_repayment < 3 THEN 'high_risk_quick_repayment'
        WHEN loan_withdrawal_ratio > 0.3 OR repayment_withdrawal_ratio > 0.3 THEN 'moderate_risk'
        WHEN total_withdrawals > 0 OR total_loans > 0 THEN 'low_risk'
        ELSE 'no_risk'
    END as risk_category
FROM client_interaction_summary
WHERE total_withdrawals > 0 OR total_loans > 0
ORDER BY total_withdrawals DESC, total_loans DESC;
