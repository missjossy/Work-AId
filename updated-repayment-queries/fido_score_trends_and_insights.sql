-- Fido Score Trends and Behavioral Insights Analysis
-- This query provides temporal analysis and behavioral patterns from first Fido scores

WITH first_loan_data AS (
    SELECT 
        client_id,
        loan_id,
        disbursementdate,
        loanamount,
        interestrate,
        REPAYMENTINSTALLMENTS,
        LN,
        loan_product_id,
        DATE_TRUNC('month', disbursementdate) as first_month,
        DATE_TRUNC('quarter', disbursementdate) as first_quarter,
        DATE_TRUNC('year', disbursementdate) as first_year,
        -- Calculate comprehensive Fido score
        CASE 
            WHEN LN = 0 THEN 100  -- First-time borrowers get highest base score
            WHEN LN BETWEEN 1 AND 5 THEN 95
            WHEN LN BETWEEN 6 AND 11 THEN 90
            WHEN LN BETWEEN 12 AND 19 THEN 85
            WHEN LN BETWEEN 20 AND 23 THEN 80
            ELSE 75
        END +
        CASE 
            WHEN loanamount <= 500 THEN 20
            WHEN loanamount <= 1000 THEN 15
            WHEN loanamount <= 2500 THEN 10
            WHEN loanamount <= 5000 THEN 5
            WHEN loanamount <= 10000 THEN 0
            WHEN loanamount <= 20000 THEN -5
            ELSE -10
        END +
        CASE 
            WHEN interestrate <= 2.0 THEN 15
            WHEN interestrate <= 2.5 THEN 10
            WHEN interestrate <= 3.0 THEN 5
            WHEN interestrate <= 3.5 THEN 0
            WHEN interestrate <= 4.0 THEN -5
            ELSE -10
        END +
        CASE 
            WHEN REPAYMENTINSTALLMENTS <= 3 THEN 10
            WHEN REPAYMENTINSTALLMENTS <= 6 THEN 5
            WHEN REPAYMENTINSTALLMENTS <= 12 THEN 0
            WHEN REPAYMENTINSTALLMENTS <= 18 THEN -5
            ELSE -10
        END +
        CASE 
            WHEN loan_product_id LIKE '%UCBLL%' THEN 5
            ELSE 0
        END as fido_score
    FROM GHANA_PROD.ML.LOAN_INFO_TBL
    WHERE disbursementdate IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY disbursementdate ASC) = 1
),

score_analysis AS (
    SELECT 
        *,
        CASE 
            WHEN fido_score >= 120 THEN 'Excellent'
            WHEN fido_score >= 100 THEN 'Good'
            WHEN fido_score >= 80 THEN 'Fair'
            WHEN fido_score >= 60 THEN 'Poor'
            ELSE 'Very Poor'
        END as score_category,
        CASE 
            WHEN fido_score >= 100 THEN 'Low Risk'
            WHEN fido_score >= 80 THEN 'Medium Risk'
            ELSE 'High Risk'
        END as risk_level,
        CASE 
            WHEN loanamount <= 1000 THEN 'Micro Loan'
            WHEN loanamount <= 5000 THEN 'Small Loan'
            WHEN loanamount <= 15000 THEN 'Medium Loan'
            ELSE 'Large Loan'
        END as loan_size_category
    FROM first_loan_data
),

monthly_trends AS (
    SELECT 
        first_month,
        COUNT(*) as new_clients,
        AVG(fido_score) as avg_score,
        MEDIAN(fido_score) as median_score,
        STDDEV(fido_score) as score_stddev,
        COUNT(CASE WHEN risk_level = 'Low Risk' THEN 1 END) as low_risk_count,
        COUNT(CASE WHEN risk_level = 'Medium Risk' THEN 1 END) as medium_risk_count,
        COUNT(CASE WHEN risk_level = 'High Risk' THEN 1 END) as high_risk_count,
        AVG(loanamount) as avg_loan_amount,
        AVG(interestrate) as avg_interest_rate
    FROM score_analysis
    GROUP BY first_month
),

quarterly_trends AS (
    SELECT 
        first_quarter,
        COUNT(*) as new_clients,
        AVG(fido_score) as avg_score,
        MEDIAN(fido_score) as median_score,
        COUNT(CASE WHEN risk_level = 'Low Risk' THEN 1 END) as low_risk_count,
        COUNT(CASE WHEN risk_level = 'High Risk' THEN 1 END) as high_risk_count,
        ROUND(COUNT(CASE WHEN risk_level = 'Low Risk' THEN 1 END) * 100.0 / COUNT(*), 2) as low_risk_percentage
    FROM score_analysis
    GROUP BY first_quarter
),

product_analysis AS (
    SELECT 
        CASE 
            WHEN loan_product_id LIKE '%UCBLL%' THEN 'FidoBiz'
            ELSE 'Personal'
        END as product_type,
        COUNT(*) as client_count,
        AVG(fido_score) as avg_score,
        MEDIAN(fido_score) as median_score,
        COUNT(CASE WHEN risk_level = 'Low Risk' THEN 1 END) as low_risk_count,
        COUNT(CASE WHEN risk_level = 'High Risk' THEN 1 END) as high_risk_count,
        AVG(loanamount) as avg_loan_amount,
        AVG(interestrate) as avg_interest_rate
    FROM score_analysis
    GROUP BY product_type
),

loan_size_analysis AS (
    SELECT 
        loan_size_category,
        COUNT(*) as client_count,
        AVG(fido_score) as avg_score,
        MEDIAN(fido_score) as median_score,
        COUNT(CASE WHEN risk_level = 'Low Risk' THEN 1 END) as low_risk_count,
        COUNT(CASE WHEN risk_level = 'High Risk' THEN 1 END) as high_risk_count,
        AVG(interestrate) as avg_interest_rate
    FROM score_analysis
    GROUP BY loan_size_category
)

-- Monthly Score Trends
SELECT 
    'Monthly Score Trends' as analysis_type,
    first_month as period,
    new_clients,
    ROUND(avg_score, 2) as avg_fido_score,
    median_score as median_fido_score,
    ROUND(score_stddev, 2) as score_volatility,
    low_risk_count,
    medium_risk_count,
    high_risk_count,
    ROUND(low_risk_count * 100.0 / new_clients, 1) as low_risk_percentage,
    ROUND(avg_loan_amount, 0) as avg_loan_amount,
    ROUND(avg_interest_rate, 2) as avg_interest_rate
FROM monthly_trends
ORDER BY first_month

UNION ALL

-- Quarterly Summary
SELECT 
    'Quarterly Summary' as analysis_type,
    first_quarter as period,
    new_clients,
    ROUND(avg_score, 2) as avg_fido_score,
    median_score as median_fido_score,
    NULL as score_volatility,
    low_risk_count,
    NULL as medium_risk_count,
    high_risk_count,
    low_risk_percentage,
    NULL as avg_loan_amount,
    NULL as avg_interest_rate
FROM quarterly_trends
ORDER BY first_quarter

UNION ALL

-- Product Type Analysis
SELECT 
    'Product Analysis' as analysis_type,
    product_type as period,
    client_count as new_clients,
    ROUND(avg_score, 2) as avg_fido_score,
    median_score as median_fido_score,
    NULL as score_volatility,
    low_risk_count,
    NULL as medium_risk_count,
    high_risk_count,
    ROUND(low_risk_count * 100.0 / client_count, 1) as low_risk_percentage,
    ROUND(avg_loan_amount, 0) as avg_loan_amount,
    ROUND(avg_interest_rate, 2) as avg_interest_rate
FROM product_analysis
ORDER BY product_type

UNION ALL

-- Loan Size Analysis
SELECT 
    'Loan Size Analysis' as analysis_type,
    loan_size_category as period,
    client_count as new_clients,
    ROUND(avg_score, 2) as avg_fido_score,
    median_score as median_fido_score,
    NULL as score_volatility,
    low_risk_count,
    NULL as medium_risk_count,
    high_risk_count,
    ROUND(low_risk_count * 100.0 / client_count, 1) as low_risk_percentage,
    NULL as avg_loan_amount,
    ROUND(avg_interest_rate, 2) as avg_interest_rate
FROM loan_size_analysis
ORDER BY 
    CASE loan_size_category
        WHEN 'Micro Loan' THEN 1
        WHEN 'Small Loan' THEN 2
        WHEN 'Medium Loan' THEN 3
        WHEN 'Large Loan' THEN 4
    END; 