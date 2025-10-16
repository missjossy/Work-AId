-- Quick Diagnostic to Check Common Issues

-- 1. Check if there are any loans at all
SELECT 'Total Loans' as check_type, COUNT(*) as count
FROM UG_PROD.ML.LOAN_INFO_TBL
WHERE disbursementdate IS NOT NULL

UNION ALL

-- 2. Check if there are any loan transactions
SELECT 'Total Loan Transactions' as check_type, COUNT(*) as count
FROM UG_PROD.MAMBU.loantransaction

UNION ALL

-- 3. Check if there are any loans with positive balance
SELECT 'Loans with Positive Balance' as check_type, COUNT(*) as count
FROM (
    SELECT DISTINCT parentaccountkey
    FROM UG_PROD.MAMBU.loantransaction
    WHERE balance > 0
)

UNION ALL

-- 4. Check if there are any loans in recent months
SELECT 'Loans in Last 6 Months' as check_type, COUNT(*) as count
FROM UG_PROD.ML.LOAN_INFO_TBL
WHERE disbursementdate >= DATEADD('month', -6, CURRENT_DATE)

UNION ALL

-- 5. Check if month spine is generating dates
SELECT 'Month Spine Count' as check_type, COUNT(*) as count
FROM (
    SELECT 
        DATE_TRUNC('month', DATEADD('month', seq4(), MIN(disbursementdate))) AS month_start,
        LAST_DAY(DATEADD('month', seq4(), MIN(disbursementdate))) AS month_end
    FROM UG_PROD.ML.LOAN_INFO_TBL
    WHERE disbursementdate IS NOT NULL
    HAVING month_end <= CURRENT_DATE
)

UNION ALL

-- 6. Check if there are any loans with disbursement dates
SELECT 'Loans with Disbursement Dates' as check_type, COUNT(*) as count
FROM UG_PROD.ML.LOAN_INFO_TBL
WHERE disbursementdate IS NOT NULL

UNION ALL

-- 7. Check the date range of disbursements
SELECT 'Min Disbursement Date' as check_type, MIN(disbursementdate) as date_value
FROM UG_PROD.ML.LOAN_INFO_TBL
WHERE disbursementdate IS NOT NULL

UNION ALL

SELECT 'Max Disbursement Date' as check_type, MAX(disbursementdate) as date_value
FROM UG_PROD.ML.LOAN_INFO_TBL
WHERE disbursementdate IS NOT NULL

UNION ALL

-- 8. Check if there are any loans with recent transactions
SELECT 'Loans with Recent Transactions' as check_type, COUNT(*) as count
FROM (
    SELECT DISTINCT parentaccountkey
    FROM UG_PROD.MAMBU.loantransaction
    WHERE DATE(entrydate) >= DATEADD('month', -3, CURRENT_DATE)
);
