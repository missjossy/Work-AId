-- Age Distribution of Borrowers Report
-- Groups borrowers by age and shows count and total loan amount disbursed
-- Filter by disbursement date range
-- 
-- To change the date range, modify the dates below:
--   Replace '2024-01-01' with your start date
--   Replace '2025-12-31' with your end date

WITH date_filter AS (
    SELECT 
        DATE('2024-01-01') AS start_date,  -- Change this to your start date
        DATE('2025-12-31') AS end_date      -- Change this to your end date
),

borrower_data AS (
    SELECT 
        m.loan_id,
        m.client_id,
        m.disbursementdate,
        m.loanamount,
        c.birthdate,
        DATEDIFF('year', c.birthdate, DATE(m.disbursementdate)) AS age
    FROM UG_PROD.ML.LOAN_INFO_TBL m
    JOIN UG_PROD.MAMBU.CLIENT c ON c.ENCODEDKEY = m.CLIENT_KEY
    CROSS JOIN date_filter df
    WHERE m.disbursementdate IS NOT NULL
      AND c.birthdate IS NOT NULL
      AND DATE(m.disbursementdate) >= df.start_date
      AND DATE(m.disbursementdate) <= df.end_date
),

age_groups AS (
    SELECT 
        CASE 
            WHEN age BETWEEN 18 AND 30 THEN '18 - 30'
            WHEN age BETWEEN 31 AND 40 THEN '31 - 40'
            WHEN age BETWEEN 41 AND 50 THEN '41 - 50'
            WHEN age >= 51 THEN '51 and above'
            ELSE 'Other'
        END AS age_group,
        COUNT(DISTINCT client_id) AS no_of_borrowers,
        SUM(loanamount) AS loan_amt_disbursed
    FROM borrower_data
    WHERE age >= 18  -- Only include borrowers 18 and above
    GROUP BY age_group
)

SELECT 
    age_group AS "Age Group (yrs)",
    no_of_borrowers AS "No. Of Borrowers",
    ROUND(loan_amt_disbursed, 2) AS "Loan Amt Disbursed (GH¢)"
FROM age_groups

UNION ALL

SELECT 
    'TOTAL' AS "Age Group (yrs)",
    SUM(no_of_borrowers) AS "No. Of Borrowers",
    ROUND(SUM(loan_amt_disbursed), 2) AS "Loan Amt Disbursed (GH¢)"
FROM age_groups

ORDER BY 
    CASE 
        WHEN "Age Group (yrs)" = '18 - 30' THEN 1
        WHEN "Age Group (yrs)" = '31 - 40' THEN 2
        WHEN "Age Group (yrs)" = '41 - 50' THEN 3
        WHEN "Age Group (yrs)" = '51 and above' THEN 4
        WHEN "Age Group (yrs)" = 'TOTAL' THEN 5
        ELSE 6
    END;

