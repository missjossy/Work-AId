WITH client_industries AS (
    SELECT 
        client_id, 
        cust_location, 
        INDUSTRY,
        CASE 
            WHEN INDUSTRY IN ('Agriculture', 'Fishing') THEN 'Agriculture Forestry & Fishing'
            WHEN INDUSTRY IN ('Mining') THEN 'Mining & Quarrying'
            WHEN INDUSTRY IN ('Industry/Manufacturing', 'Artisan', 'Textiles and Garment Making') THEN 'Manufacturing'
            WHEN INDUSTRY IN ('Construction') THEN 'Construction'
            WHEN INDUSTRY IN ('Oil & Gas', 'Electricity and Utilities') THEN 'Electricity, Gas & Water'
            WHEN INDUSTRY IN ('Trade - Wholesale', 'Trade - Retail', 'Trade - Import/Export', 'Banking and Financial Services', 'Food Industry', 'Beverage Industry', 'Sea Food Industry') THEN 'Commerce & Finance'
            WHEN INDUSTRY IN ('Transportation', 'Media and Communications', 'Logistics') THEN 'Transport, Storage And Communication'
            WHEN INDUSTRY IN ('Domestic Services', 'Other Community Services', 'Advisory/Consulting') THEN 'Services'
            ELSE 'Miscellaneous' 
        END AS economic_sector,
        POSITION, 
        GENDER, 
        USE_OF_FUNDS, 
        EMPLOYMENT, 
        INCOME_VALUE, 
        DEPENDENTS,
        DATEDIFF(year, BIRTHDAY, CURRENT_DATE) AS Age,
        CASE 
            WHEN region IN ('Accra', 'Tema') THEN 'Greater Accra Region'
            WHEN region = 'Brong-Ahafo Region' THEN 
                CASE 
                    WHEN RANDOM() <= 0.4652 THEN 'Bono Region'
                    WHEN RANDOM() <= 0.7561 THEN 'Bono East Region'
                    ELSE 'Ahafo Region'
                END
            ELSE region 
        END as region
    FROM data.survey_data
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CLIENT_ID ORDER BY LOAN_DATE DESC) = 1
),

national_id AS (
    SELECT 
        USER.id, 
        BANKING_PLATFORM_ID, 
        USER.NATIONAL_ID, 
        CREATED_TIMESTAMP 
    FROM GHANA_PROD.BANKING_SERVICE.USER 
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY BANKING_PLATFORM_ID 
        ORDER BY JSON_EXTRACT_PATH_TEXT(status,'status') ASC, 
        USER.CREATED_TIMESTAMP DESC) = 1
)

SELECT 
    c.id AS customer_id,
    c.FIRSTNAME AS first_name,
    c.MIDDLENAME AS middle_name,
    c.LASTNAME AS surname,
    c.GENDER AS gender,
    ci.Age,
    ci.INCOME_VALUE AS income_range,
    ci.DEPENDENTS AS number_of_dependents,
    nid.NATIONAL_ID AS ghana_card_id,
    date(c.BIRTHDATE) AS date_of_birth,
    ci.region,
    ci.employment AS employment_status,
    ci.economic_sector,
    'Individual' AS customer_type,
    'Non Resident' AS residency,
    'No' AS related_party,
    'Others' AS institution_type
FROM GHANA_PROD.MAMBU.CLIENT c
LEFT JOIN client_industries ci ON ci.client_id = c.id
LEFT JOIN national_id nid ON nid.BANKING_PLATFORM_ID = c.id; 