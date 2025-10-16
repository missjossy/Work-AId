-- Monthly Loan Balances Query
-- Gets outstanding fees, principal balance, and interest at the end of each month for each loan
-- Automatically excludes loans when their total balance reaches zero

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
        la.interestrate as daily_rate,
        la.encodedkey as loan_account_key
    FROM UG_PROD.ML.LOAN_INFO_TBL m
    JOIN UG_PROD.MAMBU.loanaccount la ON la.encodedkey = m.loan_key
    JOIN UG_PROD.MAMBU.LOANPRODUCT lp ON lp.ENCODEDKEY = m.PRODUCTTYPEKEY
    WHERE m.disbursementdate IS NOT NULL
),

-- Get loan balances at month end
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
            entrydate,
            transactionid
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
      AND lt.principalbalance IS NOT NULL  -- Loan must have existed in this month
      AND COALESCE(lt.balance, 0) > 0  -- Only include loans with positive balance
      AND DATE(lt.entrydate) <= ms.month_end  -- Transaction must have occurred by month end
    GROUP BY ms.month_end, li.loan_id, li.loan_key, li.client_id, li.disbursed_amount, 
             li.product_id, li.daily_rate, lt.principalbalance, lt.balance, 
             pf.amount, la.ACCOUNTSUBSTATE, r.duedate
),

-- Calculate outstanding amounts (fees, interest, penalties)
outstanding_amounts AS (
    SELECT 
        lmb.report_month,
        lmb.loan_id,
        lmb.loan_key,
        -- Calculate outstanding principal
        lmb.principal_balance,
        -- Calculate outstanding fees (processing fee - fees paid)
        lmb.processing_fee - COALESCE(SUM(CASE WHEN t."type" = 'FEE_PAYMENT' 
                                               AND DATE(t.entrydate) <= lmb.report_month 
                                               THEN t.amount ELSE 0 END), 0) as outstanding_fees,
        -- Calculate outstanding interest (accrued - paid)
        COALESCE(SUM(CASE WHEN t."type" = 'INTEREST_ACCRUAL' 
                          AND DATE(t.entrydate) <= lmb.report_month 
                          THEN t.amount ELSE 0 END), 0) - 
        COALESCE(SUM(CASE WHEN t."type" = 'INTEREST_PAYMENT' 
                          AND DATE(t.entrydate) <= lmb.report_month 
                          THEN t.amount ELSE 0 END), 0) as outstanding_interest,
        -- Calculate outstanding penalties (accrued - paid)
        COALESCE(SUM(CASE WHEN t."type" = 'PENALTY_ACCRUAL' 
                          AND DATE(t.entrydate) <= lmb.report_month 
                          THEN t.amount ELSE 0 END), 0) - 
        COALESCE(SUM(CASE WHEN t."type" = 'PENALTY_PAYMENT' 
                          AND DATE(t.entrydate) <= lmb.report_month 
                          THEN t.amount ELSE 0 END), 0) as outstanding_penalties,
        -- Total outstanding balance
        lmb.principal_balance + 
        (lmb.processing_fee - COALESCE(SUM(CASE WHEN t."type" = 'FEE_PAYMENT' 
                                               AND DATE(t.entrydate) <= lmb.report_month 
                                               THEN t.amount ELSE 0 END), 0)) +
        (COALESCE(SUM(CASE WHEN t."type" = 'INTEREST_ACCRUAL' 
                           AND DATE(t.entrydate) <= lmb.report_month 
                           THEN t.amount ELSE 0 END), 0) - 
         COALESCE(SUM(CASE WHEN t."type" = 'INTEREST_PAYMENT' 
                           AND DATE(t.entrydate) <= lmb.report_month 
                           THEN t.amount ELSE 0 END), 0)) +
        (COALESCE(SUM(CASE WHEN t."type" = 'PENALTY_ACCRUAL' 
                           AND DATE(t.entrydate) <= lmb.report_month 
                           THEN t.amount ELSE 0 END), 0) - 
         COALESCE(SUM(CASE WHEN t."type" = 'PENALTY_PAYMENT' 
                           AND DATE(t.entrydate) <= lmb.report_month 
                           THEN t.amount ELSE 0 END), 0)) as total_outstanding_balance
    FROM loan_monthly_balances lmb
    LEFT JOIN UG_PROD.MAMBU.loantransaction t ON t.parentaccountkey = lmb.loan_key
    GROUP BY lmb.report_month, lmb.loan_id, lmb.loan_key, lmb.principal_balance, 
             lmb.processing_fee
)

-- Final result
SELECT 
    oa.report_month,
    TO_CHAR(li.disbursementdate, 'YYYY-MM') as month_disbursed,
    oa.loan_id,
    li.client_id,
    li.product_id,
    li.disbursed_amount,
    li.daily_rate * 365/100 as interest_rate_annual,
    li.disbursementdate,
    -- Outstanding amounts
    oa.principal_balance,
    oa.outstanding_fees,
    oa.outstanding_interest,
    oa.outstanding_penalties,
    oa.total_outstanding_balance,
    -- Additional information
    lmb.total_repayments,
    lmb.is_loan_locked,
    lmb.due_date,
    lmb.days_past_due,
    -- PAR classification based on days past due
    CASE 
        WHEN lmb.days_past_due IS NULL OR lmb.days_past_due <= 2 THEN 'Performing'
        WHEN lmb.days_past_due BETWEEN 3 AND 32 THEN 'Watch'
        WHEN lmb.days_past_due BETWEEN 33 AND 92 THEN 'Substandard'
        WHEN lmb.days_past_due BETWEEN 93 AND 182 THEN 'Doubtful'
        WHEN lmb.days_past_due > 182 THEN 'Loss'
        ELSE 'Performing'
    END as par_classification
FROM outstanding_amounts oa
JOIN loan_info li ON oa.loan_id = li.loan_id
JOIN loan_monthly_balances lmb ON oa.loan_id = lmb.loan_id AND oa.report_month = lmb.report_month
-- Only include records where total outstanding balance > 0
WHERE oa.total_outstanding_balance > 0
ORDER BY oa.loan_id, oa.report_month;
