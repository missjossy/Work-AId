-- Unified Persona Savings Query
-- Filter by persona type: 'Mature Professionals', 'High-Income Earners', or 'Urban Professionals'
-- Use {{persona_type}} parameter to filter

WITH client_data AS (
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
        ds.region_normalized,
        ds.income_numeric,
        CASE WHEN li.client_id IS NOT NULL THEN 'Loan Customer' ELSE 'New Saver' END AS user_type,
        -- Persona classification
        CASE
            WHEN ds.age BETWEEN 45 AND 54
                 AND ds.INCOME_VALUE = 'Above 1800 GHS'
                 AND ds.EDUCATION_LEVEL IN ('Masters', 'A-Level/SHS', 'A-Level', 'SHS')
                 AND ds.region_normalized = 'Greater Accra Region'
                 AND ds.EMPLOYMENT IN ('Full-time employed', 'Self-employed')
                 AND li.client_id IS NOT NULL
            THEN 'Mature Professionals'
            WHEN ds.age BETWEEN 25 AND 44
                 AND ds.INCOME_VALUE = 'Above 1800 GHS'
                 AND (ds.EDUCATION_LEVEL LIKE '%University%' 
                      OR ds.EDUCATION_LEVEL IN ('University', 'Masters', 'Bachelors'))
                 AND ds.EMPLOYMENT IN ('Full-time employed', 'Self-employed')
            THEN 'High-Income Earners'
            WHEN ds.age BETWEEN 25 AND 34
                 AND (ds.income_numeric >= 701 OR ds.INCOME_VALUE IN ('701 GHS - 1000 GHS', '1001 GHS - 1400 GHS', '1401 GHS - 1800 GHS', 'Above 1800 GHS'))
                 AND ds.EDUCATION_LEVEL IN ('A-Level/SHS', 'A-Level', 'SHS', 'University', 'Bachelors', 'Masters')
                 AND ds.region_normalized = 'Greater Accra Region'
                 AND ds.EMPLOYMENT = 'Full-time employed'
                 AND li.client_id IS NOT NULL
            THEN 'Urban Professionals'
            ELSE 'Other'
        END AS persona_type
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
)

SELECT 
    client_id,
    current_score,
    accountstate, 
    LN, 
    firstname, 
    lastname, 
    gender, 
    age, 
    industry,
    position, 
    mobilephone, 
    altphone,
    region,
    INCOME_VALUE,
    EDUCATION_LEVEL,
    EMPLOYMENT,
    user_type,
    persona_type
FROM client_data
WHERE persona_type = '{{persona_type}}'
  -- Account state filters (conditional based on persona)
  AND (
      -- Mature Professionals and Urban Professionals require loan customers
      (persona_type IN ('Mature Professionals', 'Urban Professionals')
       AND accountstate != 'CLOSED_WRITTEN_OFF'
       AND accountstate IN ({{accountstates}}))
      OR
      -- High-Income Earners allows both user types
      (persona_type = 'High-Income Earners'
       AND (accountstate != 'CLOSED_WRITTEN_OFF' OR accountstate IS NULL)
       AND (accountstate IN ({{accountstates}}) OR accountstate IS NULL))
  )

