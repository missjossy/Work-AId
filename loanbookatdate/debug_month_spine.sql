-- Debug the month spine issue
-- The month spine is only generating 1 month, which is causing the problem

-- 1. Check the disbursement date range
SELECT 
    'Date Range' as check_type,
    MIN(disbursementdate) as min_date,
    MAX(disbursementdate) as max_date,
    COUNT(*) as total_loans
FROM UG_PROD.ML.LOAN_INFO_TBL
WHERE disbursementdate IS NOT NULL

UNION ALL

-- 2. Check what the month spine is actually generating (using the same logic as fixed query)
SELECT 
    'Month Spine Debug' as check_type,
    month_start as min_date,
    month_end as max_date,
    COUNT(*) as month_count
FROM (
    SELECT 
        DATEADD('month', seq4(), '2020-01-01') AS month_start,
        LAST_DAY(DATEADD('month', seq4(), '2020-01-01')) AS month_end
    FROM TABLE(GENERATOR(ROWCOUNT => 60))  -- Generate 60 months (5 years)
    WHERE month_end <= CURRENT_DATE
      AND month_end >= '2020-01-01'  -- Start from 2020
)

UNION ALL

-- 3. Check if GENERATOR is working properly
SELECT 
    'Generator Test' as check_type,
    seq4() as seq_value,
    DATEADD('month', seq4(), '2020-01-01') as test_date,
    COUNT(*) as count
FROM TABLE(GENERATOR(ROWCOUNT => 12))  -- Generate 12 months

UNION ALL

-- 4. Check what happens with a fixed date range
SELECT 
    'Fixed Month Range' as check_type,
    DATE_TRUNC('month', '2020-01-01') as min_date,
    LAST_DAY('2024-12-31') as max_date,
    COUNT(*) as month_count
FROM (
    SELECT 
        DATEADD('month', seq4(), '2020-01-01') AS month_start,
        LAST_DAY(DATEADD('month', seq4(), '2020-01-01')) AS month_end
    FROM TABLE(GENERATOR(ROWCOUNT => 60))  -- Generate 60 months
    WHERE month_end <= CURRENT_DATE
)

UNION ALL

-- 5. Check the actual disbursement dates
SELECT 
    'Sample Disbursement Dates' as check_type,
    disbursementdate as sample_date,
    COUNT(*) as count
FROM UG_PROD.ML.LOAN_INFO_TBL
WHERE disbursementdate IS NOT NULL
GROUP BY disbursementdate
ORDER BY disbursementdate
LIMIT 5

UNION ALL

-- 6. Test the exact month spine logic from the fixed query
SELECT 
    'Exact Month Spine Test' as check_type,
    MIN(month_start) as min_date,
    MAX(month_end) as max_date,
    COUNT(*) as month_count
FROM (
    SELECT 
        DATEADD('month', seq4(), '2020-01-01') AS month_start,
        LAST_DAY(DATEADD('month', seq4(), '2020-01-01')) AS month_end
    FROM TABLE(GENERATOR(ROWCOUNT => 60))  -- Generate 60 months (5 years)
    WHERE month_end <= CURRENT_DATE
      AND month_end >= '2020-01-01'  -- Start from 2020
);
