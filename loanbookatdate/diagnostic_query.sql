-- Diagnostic Query to Identify Why Main Query Returns No Results
-- This will help identify where data is being filtered out

WITH 
-- Generate month-end dates from earliest loan to current date
month_spine AS (
    SELECT 
        DATE_TRUNC('month', DATEADD('month', seq4(), MIN(disbursementdate))) AS month_start,
        LAST_DAY(DATEADD('month', seq4(), MIN(disbursementdate))) AS month_end
    FROM UG_PROD.ML.LOAN_INFO_TBL
    WHERE disbursementdate IS NOT NULL
    HAVING month_end <= CURRENT_DATE
),

-- Get basic loan information
loan_info AS (
    SELECT 
        m.loan_id,
        m.loan_key,
        m.client_id,
        m.disbursementdate,
        m.loanamount as disbursed_amount,
        lp.id AS product_id,
        la.interestrate as daily_rate
    FROM UG_PROD.ML.LOAN_INFO_TBL m
    JOIN UG_PROD.MAMBU.loanaccount la ON la.encodedkey = m.loan_key
    JOIN UG_PROD.MAMBU.LOANPRODUCT lp ON lp.ENCODEDKEY = m.PRODUCTTYPEKEY
    WHERE m.disbursementdate IS NOT NULL
),

-- Get loan balances at month end (without strict filtering)
loan_monthly_balances AS (
    SELECT 
        ms.month_end as report_month,
        li.loan_id,
        li.loan_key,
        li.client_id,
        li.disbursed_amount,
        li.product_id,
        li.daily_rate,
        -- Get the latest transaction state as of month end
        COALESCE(lt.principalbalance, 0) as principal_balance,
        COALESCE(lt.balance, 0) as total_balance,
        -- Calculate total repayments up to this month
        COALESCE(SUM(CASE WHEN t."type" IN ('REPAYMENT', 'REPAYMENT_ADJUSTMENT') 
                           AND DATE(t.entrydate) <= ms.month_end 
                           THEN t.amount ELSE 0 END), 0) as total_repayments,
        -- Get processing fee
        COALESCE(pf.amount, 0) as processing_fee,
        -- Get loan status
        CASE WHEN la.ACCOUNTSUBSTATE = 'LOCKED' THEN TRUE ELSE FALSE END AS is_loan_locked,
        -- Get latest due date
        r.duedate as due_date,
        -- Calculate days past due
        CASE WHEN r.duedate IS NOT NULL 
             THEN DATEDIFF('day', r.duedate, ms.month_end) 
             ELSE NULL END as days_past_due
    FROM month_spine ms
    CROSS JOIN loan_info li
    -- Get the most recent transaction state as of month end
    LEFT JOIN (
        SELECT 
            parentaccountkey, 
            principalbalance, 
            balance, 
            entrydate
        FROM UG_PROD.MAMBU.loantransaction
        QUALIFY ROW_NUMBER() OVER (PARTITION BY parentaccountkey ORDER BY transactionid DESC) = 1
    ) lt ON lt.parentaccountkey = li.loan_key
    -- Get all transactions for repayment calculation
    LEFT JOIN UG_PROD.MAMBU.loantransaction t ON t.parentaccountkey = li.loan_key
    -- Get processing fee
    LEFT JOIN UG_PROD.MAMBU.PredefinedFee pf ON pf.loanfees_encodedkey_own = li.loan_key 
                                               AND pf.name = 'Processing Fee'
    -- Get loan account for status
    LEFT JOIN UG_PROD.MAMBU.LOANACCOUNT la ON la.encodedkey = li.loan_key
    -- Get repayment schedule for due date (latest due date as of month end)
    LEFT JOIN (
        SELECT 
            PARENTACCOUNTKEY, 
            duedate
        FROM UG_PROD.MAMBU.REPAYMENT
        QUALIFY ROW_NUMBER() OVER (PARTITION BY PARENTACCOUNTKEY ORDER BY duedate DESC) = 1
    ) r ON r.PARENTACCOUNTKEY = li.loan_key
    WHERE ms.month_end >= DATE_TRUNC('month', li.disbursementdate)  -- Only include months after disbursement
    GROUP BY ms.month_end, li.loan_id, li.loan_key, li.client_id, li.disbursed_amount, 
             li.product_id, li.daily_rate, lt.principalbalance, lt.balance, 
             pf.amount, la.ACCOUNTSUBSTATE, r.duedate
)

-- Diagnostic Results - Check each step
SELECT '1. Month Spine' as step, COUNT(*) as record_count, 
       MIN(month_end) as min_month, MAX(month_end) as max_month
FROM month_spine

UNION ALL

SELECT '2. Loan Info' as step, COUNT(*) as record_count,
       MIN(disbursementdate) as min_date, MAX(disbursementdate) as max_date
FROM loan_info

UNION ALL

SELECT '3. Monthly Balances (All)' as step, COUNT(*) as record_count,
       MIN(report_month) as min_month, MAX(report_month) as max_month
FROM loan_monthly_balances

UNION ALL

SELECT '4. Monthly Balances (With Principal)' as step, COUNT(*) as record_count,
       MIN(report_month) as min_month, MAX(report_month) as max_month
FROM loan_monthly_balances
WHERE principal_balance IS NOT NULL

UNION ALL

SELECT '5. Monthly Balances (Positive Balance)' as step, COUNT(*) as record_count,
       MIN(report_month) as min_month, MAX(report_month) as max_month
FROM loan_monthly_balances
WHERE principal_balance IS NOT NULL AND total_balance > 0

UNION ALL

SELECT '6. Sample Records' as step, COUNT(*) as record_count,
       MIN(report_month) as min_month, MAX(report_month) as max_month
FROM loan_monthly_balances
WHERE principal_balance IS NOT NULL AND total_balance > 0
LIMIT 5;
