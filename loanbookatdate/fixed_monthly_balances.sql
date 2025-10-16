WITH 
-- Generate month-end dates using a more reliable method
month_spine AS (
    SELECT 
        DATEADD('month', seq4(), '2020-01-01') AS month_start,
        LAST_DAY(DATEADD('month', seq4(), '2020-01-01')) AS month_end
    FROM TABLE(GENERATOR(ROWCOUNT => 60))  -- Generate 60 months (5 years)
    WHERE month_end <= CURRENT_DATE
      AND month_end >= '2020-01-01'  -- Start from 2020
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

-- Get loan balances at month end (simplified approach)
loan_monthly_balances AS (
    SELECT 
        ms.month_end as report_month,
        li.loan_id,
        li.loan_key,
        li.client_id,
        li.disbursed_amount,
        li.disbursementdate,
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
    WHERE ms.month_end >= DATE_TRUNC('month', DATE(li.disbursementdate))  -- Only include months after disbursement
    GROUP BY ms.month_end, li.loan_id, li.loan_key, li.client_id, li.disbursed_amount, 
             li.disbursementdate, li.product_id, li.daily_rate, lt.principalbalance, lt.balance, 
             pf.amount, la.ACCOUNTSUBSTATE, r.duedate
)

-- Final output - all monthly balances ready for Tableau filtering
SELECT 
    report_month,
    loan_id,
    loan_key,
    client_id,
    disbursed_amount,
    product_id,
    daily_rate,
    principal_balance as outstanding_principal,
    (total_balance - principal_balance) as outstanding_fees,
    total_balance as total_outstanding_balance,
    total_repayments,
    processing_fee,
    is_loan_locked,
    due_date,
    days_past_due,
    -- Add PAR classification
    CASE 
        WHEN days_past_due IS NULL OR days_past_due <= 0 THEN 'Performing'
        WHEN days_past_due BETWEEN 1 AND 30 THEN 'Watch'
        WHEN days_past_due BETWEEN 31 AND 90 THEN 'Substandard'
        WHEN days_past_due BETWEEN 91 AND 180 THEN 'Doubtful'
        WHEN days_past_due > 180 THEN 'Loss'
        ELSE 'Performing'
    END as par_classification,
    -- Flag if loan should be excluded (fully paid)
    CASE WHEN total_balance <= 0 THEN TRUE ELSE FALSE END as is_fully_paid
FROM loan_monthly_balances
WHERE total_balance > 0  -- Only include loans with outstanding balance
ORDER BY report_month DESC, loan_id;
