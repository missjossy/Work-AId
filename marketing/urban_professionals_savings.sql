-- Urban Professionals Persona (Greater Accra)
-- Criteria: Age 25-34, Income 701+ GHS, Education A-Level/University,
--           Region Greater Accra, Employment Full-time Employed, Loan Customers
SELECT 
    cl.client_id,
    fs.score AS current_score,
    li.accountstate, 
    li.LN, 
    ds.firstname, 
    ds.lastname, 
    ds.gender, 
    ds.age, 
    ds.industry,
    ds.position, 
    ds.mobilephone, 
    ds.altphone,
    ds.region,
    ds.INCOME_VALUE,
    ds.EDUCATION_LEVEL,
    ds.EMPLOYMENT
FROM ml.client_info cl
LEFT JOIN (
    SELECT * 
    FROM ml.loan_info_tbl
    QUALIFY ROW_NUMBER() OVER(PARTITION BY client_id ORDER BY disbursementdate, LN DESC) = 1
) li ON li.client_id = cl.client_id
LEFT JOIN (
    SELECT * 
    FROM data.fido_score
    QUALIFY ROW_NUMBER() OVER(PARTITION BY client_id ORDER BY created_on DESC) = 1
) fs ON cl.client_id = fs.client_id
LEFT JOIN (
    SELECT *,
        CASE 
            WHEN region IN ('Accra', 'Tema') THEN 'Greater Accra Region'
            WHEN region = 'Other Region' THEN cust_location || ' ' || 'Region' 
            ELSE region 
        END AS region_normalized,
        CASE 
            WHEN INCOME_VALUE = 'Below 350 GHS' THEN 175
            WHEN INCOME_VALUE = '351 GHS - 700 GHS' THEN 525
            WHEN INCOME_VALUE = '701 GHS - 1000 GHS' THEN 850
            WHEN INCOME_VALUE = '1001 GHS - 1400 GHS' THEN 1200
            WHEN INCOME_VALUE = '1401 GHS - 1800 GHS' THEN 1600
            WHEN INCOME_VALUE = 'Above 1800 GHS' THEN 2000
            ELSE NULL
        END AS income_numeric
    FROM data.survey_data
    QUALIFY ROW_NUMBER() OVER(PARTITION BY client_id ORDER BY loan_date DESC) = 1
) ds ON ds.client_id = cl.client_id
WHERE cl.client_id NOT IN (
    SELECT DISTINCT cl.id 
    FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa
    LEFT JOIN MAMBU.CLIENT cl ON cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
    WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
      AND accountstate != 'WITHDRAWN'
)
AND BUSINESS_RULE_DECISION = 'APPROVED'
AND fs.score > ({{score_limit}})
AND (fraud_type_matched = '' OR fraud_type_matched IS NULL)
AND DATE(fs.created_on) BETWEEN DATE('{{ Range.start }}') AND DATE('{{ Range.end }}')
AND li.accountstate != 'CLOSED_WRITTEN_OFF'
AND li.accountstate IN ({{accountstates}})
-- Urban Professionals Persona Filters
AND ds.age BETWEEN 25 AND 34
AND (ds.income_numeric >= 701 OR ds.INCOME_VALUE IN ('701 GHS - 1000 GHS', '1001 GHS - 1400 GHS', '1401 GHS - 1800 GHS', 'Above 1800 GHS'))
AND ds.EDUCATION_LEVEL IN ('A-Level/SHS', 'A-Level', 'SHS', 'University', 'Bachelors', 'Masters')
AND ds.region_normalized = 'Greater Accra Region'
AND ds.EMPLOYMENT = 'Full-time employed'
AND li.client_id IS NOT NULL  -- Loan customers only

