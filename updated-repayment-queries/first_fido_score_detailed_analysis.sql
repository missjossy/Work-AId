-- Detailed First Fido Score Analysis
-- This query analyzes the first Fido score of clients based on their initial loan characteristics

WITH client_first_interaction AS (
    SELECT 
        client_id,
        loan_id,
        disbursementdate,
        loanamount,
        interestrate,
        REPAYMENTINSTALLMENTS,
        LN,
        loan_product_id,
        DATE_TRUNC('month', disbursementdate) as first_loan_month,
        DATE_TRUNC('quarter', disbursementdate) as first_loan_quarter,
        DATE_TRUNC('year', disbursementdate) as first_loan_year
    FROM GHANA_PROD.ML.LOAN_INFO_TBL
    WHERE disbursementdate IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY disbursementdate ASC) = 1
),

fido_score_components AS (
    SELECT 
        *,
        -- Base score component (100 points max)
        CASE 
            WHEN LN = 0 THEN 100  -- First-time borrowers
            WHEN LN BETWEEN 1 AND 5 THEN 95
            WHEN LN BETWEEN 6 AND 11 THEN 90
            WHEN LN BETWEEN 12 AND 19 THEN 85
            WHEN LN BETWEEN 20 AND 23 THEN 80
            ELSE 75
        END as base_score,
        
        -- Loan amount risk component (20 points max)
        CASE 
            WHEN loanamount <= 500 THEN 20
            WHEN loanamount <= 1000 THEN 15
            WHEN loanamount <= 2500 THEN 10
            WHEN loanamount <= 5000 THEN 5
            WHEN loanamount <= 10000 THEN 0
            WHEN loanamount <= 20000 THEN -5
            ELSE -10
        END as amount_score,
        
        -- Interest rate component (15 points max)
        CASE 
            WHEN interestrate <= 2.0 THEN 15
            WHEN interestrate <= 2.5 THEN 10
            WHEN interestrate <= 3.0 THEN 5
            WHEN interestrate <= 3.5 THEN 0
            WHEN interestrate <= 4.0 THEN -5
            ELSE -10
        END as rate_score,
        
        -- Loan term component (10 points max)
        CASE 
            WHEN REPAYMENTINSTALLMENTS <= 3 THEN 10
            WHEN REPAYMENTINSTALLMENTS <= 6 THEN 5
            WHEN REPAYMENTINSTALLMENTS <= 12 THEN 0
            WHEN REPAYMENTINSTALLMENTS <= 18 THEN -5
            ELSE -10
        END as term_score,
        
        -- Product type component (5 points max)
        CASE 
            WHEN loan_product_id LIKE '%UCBLL%' THEN 5  -- FidoBiz customers
            ELSE 0
        END as product_score
    FROM client_first_interaction
),

calculated_fido_scores AS (
    SELECT 
        *,
        (base_score + amount_score + rate_score + term_score + product_score) as fido_score,
        CASE 
            WHEN fido_score >= 120 THEN 'Excellent (120+)'
            WHEN fido_score >= 100 THEN 'Good (100-119)'
            WHEN fido_score >= 80 THEN 'Fair (80-99)'
            WHEN fido_score >= 60 THEN 'Poor (60-79)'
            ELSE 'Very Poor (<60)'
        END as fido_score_band,
        CASE 
            WHEN fido_score >= 100 THEN 'Low Risk'
            WHEN fido_score >= 80 THEN 'Medium Risk'
            ELSE 'High Risk'
        END as risk_category
    FROM fido_score_components
),

monthly_score_trends AS (
    SELECT 
        first_loan_month,
        COUNT(*) as new_clients,
        AVG(fido_score) as avg_fido_score,
        MEDIAN(fido_score) as median_fido_score,
        STDDEV(fido_score) as score_volatility,
        COUNT(CASE WHEN risk_category = 'Low Risk' THEN 1 END) as low_risk_clients,
        COUNT(CASE WHEN risk_category = 'Medium Risk' THEN 1 END) as medium_risk_clients,
        COUNT(CASE WHEN risk_category = 'High Risk' THEN 1 END) as high_risk_clients
    FROM calculated_fido_scores
    GROUP BY first_loan_month
),

score_distribution AS (
    SELECT 
        fido_score_band,
        risk_category,
        COUNT(*) as client_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
        AVG(loanamount) as avg_loan_amount,
        AVG(interestrate) as avg_interest_rate,
        AVG(REPAYMENTINSTALLMENTS) as avg_loan_term
    FROM calculated_fido_scores
    GROUP BY fido_score_band, risk_category
)

-- Main Analysis Results
SELECT 'Overall First Fido Score Summary' as analysis_section, '' as metric, '' as value, '' as details
FROM calculated_fido_scores
LIMIT 1

UNION ALL

SELECT 
    'Score Statistics' as analysis_section,
    'Total First-Time Clients' as metric,
    CAST(COUNT(*) AS VARCHAR) as value,
    'All clients with their first loan' as details
FROM calculated_fido_scores

UNION ALL

SELECT 
    'Score Statistics' as analysis_section,
    'Average Fido Score' as metric,
    CAST(ROUND(AVG(fido_score), 2) AS VARCHAR) as value,
    'Mean score across all first-time clients' as details
FROM calculated_fido_scores

UNION ALL

SELECT 
    'Score Statistics' as analysis_section,
    'Median Fido Score' as metric,
    CAST(MEDIAN(fido_score) AS VARCHAR) as value,
    'Middle score when ranked' as details
FROM calculated_fido_scores

UNION ALL

SELECT 
    'Score Statistics' as analysis_section,
    'Score Range' as metric,
    CONCAT(CAST(MIN(fido_score) AS VARCHAR), ' - ', CAST(MAX(fido_score) AS VARCHAR)) as value,
    'Minimum to maximum score range' as details
FROM calculated_fido_scores

UNION ALL

SELECT 
    'Risk Distribution' as analysis_section,
    'Low Risk Clients' as metric,
    CONCAT(CAST(COUNT(CASE WHEN risk_category = 'Low Risk' THEN 1 END) AS VARCHAR), 
           ' (', CAST(ROUND(COUNT(CASE WHEN risk_category = 'Low Risk' THEN 1 END) * 100.0 / COUNT(*), 1) AS VARCHAR), '%)') as value,
    'Clients with score >= 100' as details
FROM calculated_fido_scores

UNION ALL

SELECT 
    'Risk Distribution' as analysis_section,
    'Medium Risk Clients' as metric,
    CONCAT(CAST(COUNT(CASE WHEN risk_category = 'Medium Risk' THEN 1 END) AS VARCHAR),
           ' (', CAST(ROUND(COUNT(CASE WHEN risk_category = 'Medium Risk' THEN 1 END) * 100.0 / COUNT(*), 1) AS VARCHAR), '%)') as value,
    'Clients with score 80-99' as details
FROM calculated_fido_scores

UNION ALL

SELECT 
    'Risk Distribution' as analysis_section,
    'High Risk Clients' as metric,
    CONCAT(CAST(COUNT(CASE WHEN risk_category = 'High Risk' THEN 1 END) AS VARCHAR),
           ' (', CAST(ROUND(COUNT(CASE WHEN risk_category = 'High Risk' THEN 1 END) * 100.0 / COUNT(*), 1) AS VARCHAR), '%)') as value,
    'Clients with score < 80' as details
FROM calculated_fido_scores

UNION ALL

SELECT 
    'Product Analysis' as analysis_section,
    'FidoBiz vs Personal' as metric,
    CONCAT('FidoBiz: ', CAST(COUNT(CASE WHEN loan_product_id LIKE '%UCBLL%' THEN 1 END) AS VARCHAR),
           ' | Personal: ', CAST(COUNT(CASE WHEN loan_product_id NOT LIKE '%UCBLL%' THEN 1 END) AS VARCHAR)) as value,
    'Distribution by loan product type' as details
FROM calculated_fido_scores

ORDER BY analysis_section, metric; 