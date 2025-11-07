-- High-Income Earners Persona (Above 1800 GHS)
-- Criteria: Age 25-44, Income Above 1800 GHS, Education University Preferred,
--           Region Multiple Regions, Employment Full-time/Self-employed, Both User Types
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
    ds.EMPLOYMENT,
    CASE WHEN li.client_id IS NOT NULL THEN 'Loan Customer' ELSE 'New Saver' END AS user_type
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
        END AS region_normalized
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
AND (li.accountstate != 'CLOSED_WRITTEN_OFF' OR li.accountstate IS NULL)  -- Allow both loan customers and new savers
AND (li.accountstate IN ({{accountstates}}) OR li.accountstate IS NULL)  -- Both user types allowed
-- High-Income Earners Persona Filters
AND ds.age BETWEEN 25 AND 44
AND ds.INCOME_VALUE = 'Above 1800 GHS'
AND (ds.EDUCATION_LEVEL LIKE '%University%' 
     OR ds.EDUCATION_LEVEL IN ('University', 'Masters', 'Bachelors'))
AND ds.EMPLOYMENT IN ('Full-time employed', 'Self-employed')
-- Both user types allowed (loan customers and new savers)
-- No specific region restriction - multiple regions allowed

