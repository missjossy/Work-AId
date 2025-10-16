WITH client_first_loan AS (
    SELECT 
        client_id,
        loan_id,
        disbursementdate,
        loanamount,
        interestrate,
        REPAYMENTINSTALLMENTS,
        LN,
        loan_product_id,
        -- Calculate a basic risk score based on available data
        CASE 
            WHEN LN BETWEEN 0 AND 5 THEN 100  -- New customers get base score
            WHEN LN BETWEEN 6 AND 11 THEN 95
            WHEN LN BETWEEN 12 AND 19 THEN 90
            WHEN LN BETWEEN 20 AND 23 THEN 85
            ELSE 80
        END as base_score,
        -- Adjust score based on loan amount (lower amounts might indicate lower risk)
        CASE 
            WHEN loanamount <= 1000 THEN 10
            WHEN loanamount <= 5000 THEN 5
            WHEN loanamount <= 10000 THEN 0
            WHEN loanamount <= 20000 THEN -5
            ELSE -10
        END as amount_adjustment,
        -- Adjust score based on interest rate (lower rates might indicate better credit)
        CASE 
            WHEN interestrate <= 2.5 THEN 10
            WHEN interestrate <= 3.0 THEN 5
            WHEN interestrate <= 3.5 THEN 0
            WHEN interestrate <= 4.0 THEN -5
            ELSE -10
        END as rate_adjustment,
        -- Adjust score based on loan term (shorter terms might indicate lower risk)
        CASE 
            WHEN REPAYMENTINSTALLMENTS <= 6 THEN 5
            WHEN REPAYMENTINSTALLMENTS <= 12 THEN 0
            WHEN REPAYMENTINSTALLMENTS <= 18 THEN -5
            ELSE -10
        END as term_adjustment
    FROM GHANA_PROD.ML.LOAN_INFO_TBL
    WHERE disbursementdate IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY disbursementdate ASC) = 1
),

client_demographics AS (
    SELECT 
        client_id,
        AGE,
        GENDER,
        INCOME_VALUE,
        EMPLOYMENT,
        economic_sector,
        region
    FROM data.survey_data
    QUALIFY ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY LOAN_DATE DESC) = 1
),

fido_score_calculation AS (
    SELECT 
        cfl.*,
        cd.AGE,
        cd.GENDER,
        cd.INCOME_VALUE,
        cd.EMPLOYMENT,
        cd.economic_sector,
        cd.region,
        -- Calculate final Fido score
        (cfl.base_score + cfl.amount_adjustment + cfl.rate_adjustment + cfl.term_adjustment) as calculated_fido_score,
        -- Demographic adjustments
        CASE 
            WHEN cd.AGE BETWEEN 25 AND 45 THEN 5  -- Prime working age
            WHEN cd.AGE BETWEEN 18 AND 24 THEN 0
            WHEN cd.AGE BETWEEN 46 AND 60 THEN 0
            ELSE -5
        END as age_adjustment,
        CASE 
            WHEN cd.INCOME_VALUE IN ('High', 'Very High') THEN 10
            WHEN cd.INCOME_VALUE = 'Medium' THEN 5
            WHEN cd.INCOME_VALUE = 'Low' THEN 0
            ELSE -5
        END as income_adjustment,
        CASE 
            WHEN cd.EMPLOYMENT = 'Employed' THEN 5
            WHEN cd.EMPLOYMENT = 'Self Employed' THEN 0
            WHEN cd.EMPLOYMENT = 'Unemployed' THEN -10
            ELSE -5
        END as employment_adjustment
    FROM client_first_loan cfl
    LEFT JOIN client_demographics cd ON cd.client_id = cfl.client_id
),

final_fido_scores AS (
    SELECT 
        *,
        (calculated_fido_score + age_adjustment + income_adjustment + employment_adjustment) as final_fido_score,
        CASE 
            WHEN final_fido_score >= 90 THEN 'Excellent'
            WHEN final_fido_score >= 80 THEN 'Good'
            WHEN final_fido_score >= 70 THEN 'Fair'
            WHEN final_fido_score >= 60 THEN 'Poor'
            ELSE 'Very Poor'
        END as fido_score_category
    FROM fido_score_calculation
)

-- Main analysis query
SELECT 
    'First Fido Score Analysis' as analysis_type,
    COUNT(*) as total_clients,
    AVG(final_fido_score) as avg_fido_score,
    MEDIAN(final_fido_score) as median_fido_score,
    MIN(final_fido_score) as min_fido_score,
    MAX(final_fido_score) as max_fido_score,
    STDDEV(final_fido_score) as fido_score_stddev
FROM final_fido_scores

UNION ALL

SELECT 
    'Score Distribution by Category' as analysis_type,
    COUNT(*) as total_clients,
    AVG(final_fido_score) as avg_fido_score,
    MEDIAN(final_fido_score) as median_fido_score,
    MIN(final_fido_score) as min_fido_score,
    MAX(final_fido_score) as max_fido_score,
    STDDEV(final_fido_score) as fido_score_stddev
FROM final_fido_scores
GROUP BY fido_score_category

UNION ALL

SELECT 
    'Score by Loan Type' as analysis_type,
    COUNT(*) as total_clients,
    AVG(final_fido_score) as avg_fido_score,
    MEDIAN(final_fido_score) as median_fido_score,
    MIN(final_fido_score) as min_fido_score,
    MAX(final_fido_score) as max_fido_score,
    STDDEV(final_fido_score) as fido_score_stddev
FROM final_fido_scores
GROUP BY 
    CASE 
        WHEN loan_product_id LIKE '%UCBLL%' THEN 'FidoBiz'
        ELSE 'Personal'
    END

UNION ALL

SELECT 
    'Score by Region' as analysis_type,
    COUNT(*) as total_clients,
    AVG(final_fido_score) as avg_fido_score,
    MEDIAN(final_fido_score) as median_fido_score,
    MIN(final_fido_score) as min_fido_score,
    MAX(final_fido_score) as max_fido_score,
    STDDEV(final_fido_score) as fido_score_stddev
FROM final_fido_scores
WHERE region IS NOT NULL
GROUP BY region

UNION ALL

SELECT 
    'Score by Economic Sector' as analysis_type,
    COUNT(*) as total_clients,
    AVG(final_fido_score) as avg_fido_score,
    MEDIAN(final_fido_score) as median_fido_score,
    MIN(final_fido_score) as min_fido_score,
    MAX(final_fido_score) as max_fido_score,
    STDDEV(final_fido_score) as fido_score_stddev
FROM final_fido_scores
WHERE economic_sector IS NOT NULL
GROUP BY economic_sector

ORDER BY analysis_type, total_clients DESC; 