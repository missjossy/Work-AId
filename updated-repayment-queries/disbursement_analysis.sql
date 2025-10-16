WITH disbursement_data AS (
    SELECT 
        loan_id,
        client_id,
        disbursementdate,
        loanamount,
        LN,
        CASE 
            WHEN LN BETWEEN 0 AND 5 THEN 'LN0-5'
            WHEN LN BETWEEN 6 AND 11 THEN 'LN6-11'
            WHEN LN BETWEEN 12 AND 19 THEN 'LN12-19'
            WHEN LN BETWEEN 20 AND 23 THEN 'LN20-23'
            ELSE 'LN24+'
        END as ln_group,
        CASE 
            WHEN loan_product_id LIKE '%UCBLL%' THEN 'FidoBiz'
            ELSE 'Personal'
        END as loan_type,
        REPAYMENTINSTALLMENTS,
        interestrate,
        DATE_TRUNC('month', disbursementdate) as disbursement_month
    FROM GHANA_PROD.ML.LOAN_INFO_TBL
    WHERE disbursementdate IS NOT NULL
    AND disbursementdate >= DATEADD(year, -1, CURRENT_DATE)
),

monthly_summary AS (
    SELECT 
        disbursement_month,
        COUNT(loan_id) as total_disbursements,
        COUNT(DISTINCT client_id) as unique_clients,
        SUM(loanamount) as total_amount,
        AVG(loanamount) as avg_amount,
        MEDIAN(loanamount) as median_amount
    FROM disbursement_data
    GROUP BY disbursement_month
),

ln_group_summary AS (
    SELECT 
        ln_group,
        COUNT(loan_id) as disbursements,
        SUM(loanamount) as total_amount,
        AVG(loanamount) as avg_amount
    FROM disbursement_data
    GROUP BY ln_group
),

loan_type_summary AS (
    SELECT 
        loan_type,
        COUNT(loan_id) as disbursements,
        SUM(loanamount) as total_amount,
        AVG(loanamount) as avg_amount,
        AVG(interestrate) as avg_interest_rate
    FROM disbursement_data
    GROUP BY loan_type
)

SELECT 
    'Monthly Trends' as analysis_type,
    disbursement_month as period,
    total_disbursements,
    unique_clients,
    total_amount,
    avg_amount,
    median_amount,
    NULL as ln_group,
    NULL as loan_type
FROM monthly_summary

UNION ALL

SELECT 
    'LN Group' as analysis_type,
    ln_group as period,
    disbursements as total_disbursements,
    NULL as unique_clients,
    total_amount,
    avg_amount,
    NULL as median_amount,
    ln_group,
    NULL as loan_type
FROM ln_group_summary

UNION ALL

SELECT 
    'Loan Type' as analysis_type,
    loan_type as period,
    disbursements as total_disbursements,
    NULL as unique_clients,
    total_amount,
    avg_amount,
    NULL as median_amount,
    NULL as ln_group,
    loan_type
FROM loan_type_summary

ORDER BY analysis_type, period; 