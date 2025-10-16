-- Simple test to see what data we can get
-- This will help identify where the main query is failing

-- 1. Check if we can get basic loan info
SELECT 'Basic Loan Info' as test_step, COUNT(*) as count
FROM UG_PROD.ML.LOAN_INFO_TBL
WHERE disbursementdate IS NOT NULL

UNION ALL

-- 2. Check if we can get loan transactions
SELECT 'Loan Transactions' as test_step, COUNT(*) as count
FROM UG_PROD.MAMBU.loantransaction

UNION ALL

-- 3. Check if we can get loan accounts
SELECT 'Loan Accounts' as test_step, COUNT(*) as count
FROM UG_PROD.MAMBU.loanaccount

UNION ALL

-- 4. Check if we can get loan products
SELECT 'Loan Products' as test_step, COUNT(*) as count
FROM UG_PROD.MAMBU.LOANPRODUCT

UNION ALL

-- 5. Check if we can get basic loan info with joins
SELECT 'Joined Loan Info' as test_step, COUNT(*) as count
FROM UG_PROD.ML.LOAN_INFO_TBL m
JOIN UG_PROD.MAMBU.loanaccount la ON la.encodedkey = m.loan_key
JOIN UG_PROD.MAMBU.LOANPRODUCT lp ON lp.ENCODEDKEY = m.PRODUCTTYPEKEY
WHERE m.disbursementdate IS NOT NULL

UNION ALL

-- 6. Check if we can get month spine
SELECT 'Month Spine' as test_step, COUNT(*) as count
FROM (
    SELECT 
        DATE_TRUNC('month', DATEADD('month', seq4(), MIN(disbursementdate))) AS month_start,
        LAST_DAY(DATEADD('month', seq4(), MIN(disbursementdate))) AS month_end
    FROM UG_PROD.ML.LOAN_INFO_TBL
    WHERE disbursementdate IS NOT NULL
    HAVING month_end <= CURRENT_DATE
)

UNION ALL

-- 7. Check if we can get a simple monthly snapshot
SELECT 'Simple Monthly Snapshot' as test_step, COUNT(*) as count
FROM (
    SELECT 
        ms.month_end,
        li.loan_id,
        li.loan_key,
        li.client_id
    FROM (
        SELECT 
            DATE_TRUNC('month', DATEADD('month', seq4(), MIN(disbursementdate))) AS month_start,
            LAST_DAY(DATEADD('month', seq4(), MIN(disbursementdate))) AS month_end
        FROM UG_PROD.ML.LOAN_INFO_TBL
        WHERE disbursementdate IS NOT NULL
        HAVING month_end <= CURRENT_DATE
    ) ms
    CROSS JOIN (
        SELECT 
            m.loan_id,
            m.loan_key,
            m.client_id,
            m.disbursementdate
        FROM UG_PROD.ML.LOAN_INFO_TBL m
        WHERE m.disbursementdate IS NOT NULL
        LIMIT 10  -- Limit to first 10 loans for testing
    ) li
    WHERE ms.month_end >= DATE_TRUNC('month', DATE(li.disbursementdate))
);
