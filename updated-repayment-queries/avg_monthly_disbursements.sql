WITH monthly_disbursements AS (
    SELECT 
        DATE_TRUNC('month', disbursementdate) as month,
        COUNT(loan_id) as loans_disbursed
    FROM GHANA_PROD.ML.LOAN_INFO_TBL
    WHERE disbursementdate IS NOT NULL
    AND disbursementdate BETWEEN '2024-01-01' AND '2025-04-30'
    GROUP BY DATE_TRUNC('month', disbursementdate)
)

SELECT 
    AVG(loans_disbursed) as avg_monthly_disbursements
FROM monthly_disbursements; 