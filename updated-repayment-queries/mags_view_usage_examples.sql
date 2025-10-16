-- Example usage of the mags_report_view with different date filters

-- 1. Get current month's data (default behavior)
SELECT * FROM mags_report_view;

-- 2. Get data for a specific month by setting session parameters
-- For June 2025:
SET report_date = '2025-06-30';
SELECT * FROM mags_report_view;

-- 3. Get data for previous month
SET report_date = '2025-05-31';
SELECT * FROM mags_report_view;

-- 4. Get data for a specific quarter end
SET report_date = '2025-03-31';
SELECT * FROM mags_report_view;

-- 5. Get data for year end
SET report_date = '2024-12-31';
SELECT * FROM mags_report_view;

-- 6. Filter by specific date range in the view
SELECT * FROM mags_report_view 
WHERE "Date of Approval" >= '2024-01-01' 
  AND "Date of Approval" <= '2024-12-31';

-- 7. Get summary statistics for different months
SELECT 
    DATE_TRUNC('month', "Date of Approval") AS month,
    COUNT(*) AS loan_count,
    SUM("Loan Amount Approved") AS total_approved,
    AVG("Days in Arrears") AS avg_days_arrears
FROM mags_report_view
WHERE "Date of Approval" >= '2024-01-01'
GROUP BY 1
ORDER BY 1;

-- 8. Get PAR analysis by month
SELECT 
    DATE_TRUNC('month', "Date of Approval") AS month,
    "BOG Loan Classification",
    COUNT(*) AS loan_count,
    SUM("Loan Principal Balance (Without Interest)") AS total_balance
FROM mags_report_view
WHERE "Date of Approval" >= '2024-01-01'
GROUP BY 1, 2
ORDER BY 1, 2;

-- 9. Get regional distribution
SELECT 
    "Region",
    COUNT(*) AS loan_count,
    SUM("Loan Amount Approved") AS total_approved
FROM mags_report_view
GROUP BY 1
ORDER BY 3 DESC;

-- 10. Get economic sector analysis
SELECT 
    "Economic Sector",
    COUNT(*) AS loan_count,
    AVG("Loan Amount Approved") AS avg_loan_amount,
    AVG("Days in Arrears") AS avg_days_arrears
FROM mags_report_view
GROUP BY 1
ORDER BY 2 DESC; 