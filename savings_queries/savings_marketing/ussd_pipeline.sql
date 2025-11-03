-- Define parameters for date range
WITH
params AS (
    SELECT
        DATE('{{ Range.start }}') AS start_date, -- Start date parameter
        DATE('{{ Range.end }}') AS end_date -- End date parameter
),

-- Select distinct sign-ups within the specified date range
ussd_data AS (
    WITH
        sign_ups AS (
            SELECT DISTINCT
                DATE(CREATED_TIMESTAMP) AS signup_created_date,
                "USER".ID AS signups_id,
                REPLACE(PHONE_NUMBER, ' ','') AS PHONENUMBER, -- Clean phone number by removing spaces
                BANKING_PLATFORM_ID,
                ID
            FROM GHANA_PROD.BANKING_SERVICE."USER", params
            WHERE DATE(CREATED_TIMESTAMP) BETWEEN start_date AND end_date
        ),

        -- Select distinct USSD opt-in events within the date range and categorize source
        ussd AS (
            SELECT DISTINCT
                REPLACE(sl.PHONE_NUMBER, ' ','') AS PHONENUMBER, -- Clean phone number
                CASE
                    WHEN sl.SOURCE LIKE '%998*6%' THEN 'AirtelTigo'
                    WHEN sl.SOURCE LIKE '%998*77%' THEN 'BTL Activations'
                    WHEN sl.SOURCE LIKE '%998*7%' THEN 'TV'
                    WHEN sl.SOURCE LIKE '%998*99%' THEN 'Billboard'
                    WHEN sl.SOURCE LIKE '%998*8%' THEN 'MTN (Recharge Notifications)'
                    WHEN sl.SOURCE LIKE '%998*9%' THEN 'Radio'
                    WHEN sl.SOURCE LIKE '%998*11%' THEN 'Car Stickers'
                    WHEN sl.SOURCE LIKE '%998*44%' THEN 'MTN (Balance Check)'
                    WHEN sl.SOURCE LIKE '%998*55%' THEN 'Posters'
                    WHEN sl.SOURCE LIKE '%998*02%' THEN 'Delay'
                    WHEN sl.SOURCE LIKE '%998*01%' THEN 'Kwame Eugene'
                    WHEN sl.SOURCE LIKE '%998*5%' THEN 'Africa Talking TSMS'
                    ELSE 'Unknown'
                END AS source_name, -- Categorize source based on pattern
                DATE(sl.CREATED_TIMESTAMP) AS ussd_created_date, -- USSD event date
                id AS ussd_id
            FROM GHANA_PROD.BANKING_SERVICE.SUBSCRIPTION_LOG sl, params
            WHERE ACTION = 'opt_in'
              AND (DATE(created_timestamp) BETWEEN start_date AND end_date)
              AND source_name <> 'Unknown' -- Exclude unknown sources
            ORDER BY 1
        ),

        -- Join USSD events with sign-ups to find conversions and rank engagements
        ussd_sign_ups AS (
            SELECT
                BANKING_PLATFORM_ID client_id,
                u.PHONENUMBER,
                s.PHONENUMBER signup_phonenumber,
                SOURCE_NAME,
                ussd_created_date,
                signup_created_date,
                DATEDIFF(DAY, USSD_CREATED_DATE, SIGNUP_CREATED_DATE) days_to_conv, -- Days from USSD event to signup
                id,
                ROW_NUMBER() OVER (PARTITION BY u.PHONENUMBER ORDER BY USSD_CREATED_DATE DESC) engagement_rank -- Rank USSD engagements per phone number
            FROM ussd u
            JOIN sign_ups s ON u.PHONENUMBER = s.PHONENUMBER
                           AND USSD_CREATED_DATE <= SIGNUP_CREATED_DATE -- Ensure USSD event happened before or on signup date
            ORDER BY u.PHONENUMBER, ussd_created_date ASC
        )

    -- Select the first USSD engagement that led to a signup within the conversion date range and specific sources
    SELECT *
    FROM ussd_sign_ups
    WHERE days_to_conv BETWEEN {{Min Conversion_Date}} AND {{Max Conversion_Date}} -- Filter by conversion days
      AND engagement_rank = 1 -- Select the first engagement
      AND source_name IN ({{USSD Source Name}}) -- Filter by specified USSD sources
    ORDER BY 1
),

-- Count total unique loan attempts (based on client_key) by date and source name
total_unique_attempts AS (
    SELECT
        date_,
        source_name,
        COUNT(*) AS total_attempts
    FROM (
        SELECT DISTINCT
            TO_CHAR(DATE_TRUNC('{{Scale}}', CREATIONDATE), 'YYYY-MM-DD') date_, -- Truncate date to specified scale (e.g., day, week, month)
            ml.CLIENT_KEY,
            ml.client_id,
            source_name
        FROM GHANA_PROD.ML.LOAN_INFO_TBL ml
        JOIN ussd_data u ON u.client_id = ml.client_id -- Join with filtered USSD signup data
        JOIN params p ON 1 = 1 -- Join with parameters CTE
        WHERE DATE(ml.CREATIONDATE) BETWEEN p.start_date AND p.end_date
        ORDER BY date_
    )
    GROUP BY date_, source_name
    ORDER BY date_
),

-- Count total unique loan attempts for LN=0 (first loan) by date and source name
total_unique_attempts_ln0 AS (
    SELECT
        date_,
        source_name,
        COUNT(CLIENT_KEY) AS total_attempts
    FROM (
        SELECT DISTINCT
            TO_CHAR(DATE_TRUNC('{{Scale}}', CREATIONDATE), 'YYYY-MM-DD') date_, -- Truncate date
            ml.CLIENT_KEY,
            ml.client_id,
            source_name
        FROM GHANA_PROD.ML.LOAN_INFO_TBL ml
        JOIN ussd_data u ON u.client_id = ml.client_id -- Join with USSD signup data
        JOIN params p ON 1 = 1 -- Join with parameters CTE
        WHERE DATE(CREATIONDATE) BETWEEN p.start_date AND p.end_date
          AND LN = 0 -- Filter for first loans
        ORDER BY date_
    )
    GROUP BY date_, source_name
    ORDER BY date_
),

-- Count total survey completions by date and source name
total_survey_filled AS (
    SELECT
        date_,
        source_name,
        COUNT(*) AS survey_filled
    FROM (
        SELECT
            TO_CHAR(DATE_TRUNC('{{Scale}}', DATE(LOAN_DATE)), 'YYYY-MM-DD') date_, *
        FROM GHANA_PROD.DATA.SURVEY_DATA
        QUALIFY ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY LOAN_DATE DESC) = 1 -- Select the latest survey per client
    ) ml
    JOIN ussd_data u ON u.client_id = ml.client_id -- Join with USSD signup data
    JOIN params p ON 1 = 1 -- Join with parameters CTE
    WHERE date_ BETWEEN p.start_date AND p.end_date
    GROUP BY date_, source_name
    ORDER BY date_
),

-- Count total unique loan disbursements by date and source name
total_unique_disbursements AS (
    SELECT
        date_,
        source_name,
        COUNT(*) AS total_disbursements
    FROM (
        SELECT DISTINCT
            TO_CHAR(DATE_TRUNC('{{Scale}}', DISBURSEMENTDATE), 'YYYY-MM-DD') date_, -- Truncate date
            ml.CLIENT_KEY,
            ml.client_id,
            source_name
        FROM GHANA_PROD.ML.LOAN_INFO_TBL ml
        JOIN ussd_data u ON u.client_id = ml.client_id -- Join with USSD signup data
        JOIN params p ON 1 = 1 -- Join with parameters CTE
        WHERE DATE(DISBURSEMENTDATE) BETWEEN p.start_date AND p.end_date
        ORDER BY date_
    )
    GROUP BY date_, source_name
    ORDER BY date_
),

-- Count total unique loan disbursements for LN=0 (first loan) by product type, date, and source name
total_unique_disbursements_ln0 AS (
    SELECT
        date_,
        source_name,
        COUNT(CASE WHEN LOAN_PRODUCT_ID LIKE '%UCLL%' THEN CLIENT_KEY ELSE NULL END) AS Personal_disbursements_ln0, -- Count personal loans
        COUNT(CASE WHEN LOAN_PRODUCT_ID LIKE '%UCBLL%' THEN CLIENT_KEY ELSE NULL END) AS FidoBiz_disbursements_ln0, -- Count FidoBiz loans
        COUNT(CASE WHEN (LOAN_PRODUCT_ID LIKE '%TRSP%' OR LOAN_PRODUCT_ID LIKE '%SCTF%') THEN CLIENT_KEY ELSE NULL END) AS First_Disb_Partnerships, -- Count partnership loans
        COUNT(CLIENT_KEY) AS total_disbursements_ln0 -- Total first loans
    FROM (
        SELECT DISTINCT
            TO_CHAR(DATE_TRUNC('{{Scale}}', DISBURSEMENTDATE), 'YYYY-MM-DD') date_, -- Truncate date
            LOAN_PRODUCT_ID,
            ml.CLIENT_KEY,
            ml.client_id,
            source_name
        FROM GHANA_PROD.ML.LOAN_INFO_TBL ml
        JOIN ussd_data u ON u.client_id = ml.client_id -- Join with USSD signup data
        JOIN params p ON 1 = 1 -- Join with parameters CTE
        WHERE DATE(DISBURSEMENTDATE) BETWEEN p.start_date AND p.end_date
          AND LN = 0 -- Filter for first loans
          AND DATEDIFF(DAY, ussd_created_date, DISBURSEMENTDATE) BETWEEN {{Min Conversion_Date}} AND {{Max Conversion_Date}} -- Filter by conversion days to disbursement
        ORDER BY date_
    )
    GROUP BY date_, source_name
    ORDER BY date_
),

-- Calculate First Disbursement Repayment (FDR) rate per source and date
fdr AS (
    WITH
        -- Base query to get loan and repayment transaction details, including source_name
        base_query AS (
            SELECT
                ml.loan_id,
                ml.LN,
                re.installment,
                re.REPAYMENT_DUE_DATE,
                ml.LAST_EXPECTED_REPAYMENT,
                re.principal_due TOTAL_DISBURSMENT_AMOUNT, -- Assuming principal_due represents the installment amount due
                ml.disbursementdate,
                re.total_due,
                NVL(re.amount, 0) AS amount, -- Amount repaid, default to 0 if null
                re.transaction_date,
                u.source_name -- Include source_name from ussd_data
            FROM GHANA_PROD.ml.LOAN_INFO_TBL ml
            LEFT JOIN GHANA_PROD.ml.REPAYMENT_TRANSACTIONS_EXTENDED re ON ml.loan_id = re.loan_id -- Join with repayment transactions
            LEFT JOIN UG_PROD.ml.CLIENT_INFO cl ON cl.CLIENT_ID = re.CLIENT_ID -- Join with client info (Note: UG_PROD schema used here)
            JOIN ussd_data u ON ml.client_id = u.client_id -- Join with ussd_data to link loans to source_name
            WHERE ml.disbursementdate IS NOT NULL
              AND ml.loan_id IN ( -- Filter for loans from the first disbursement LN=0 subquery
                  SELECT DISTINCT ml_sub.loan_id
                  FROM GHANA_PROD.ML.LOAN_INFO_TBL ml_sub
                  JOIN ussd_data u_sub ON u_sub.client_id = ml_sub.client_id -- Keep this join in the subquery to filter loans based on ussd_data
                  JOIN params p_sub ON 1 = 1
                  WHERE DATE(ml_sub.DISBURSEMENTDATE) BETWEEN p_sub.start_date AND p_sub.end_date
                    AND ml_sub.LN = 0
              )
        ),

        -- Calculate amount repaid within the specified repayment window (DR), including source_name
        sub_1 AS (
            SELECT
                LN,
                loan_id,
                disbursementdate,
                COALESCE(REPAYMENT_DUE_DATE, REPAYMENT_DUE_DATE) AS DUE_DATE, -- Ensure DUE_DATE is not null (redundant COALESCE)
                TOTAL_DISBURSMENT_AMOUNT,
                installment,
                transaction_date,
                CASE WHEN DATEDIFF('day', DUE_DATE, transaction_date) <= {{ DR }} THEN amount ELSE 0 END AS amount, -- Repayment amount within DR days
                source_name -- Include source_name
            FROM base_query
            WHERE
                DATEDIFF('day', due_date, CURRENT_DATE) > {{ DR }} -- Only consider loans where the due date was more than DR days ago
                AND disbursementdate IS NOT NULL
            ORDER BY transaction_date, installment
        ),

        -- Aggregate repayment details by loan, installment, and source_name
        sub_2 AS (
            SELECT
                LN,
                loan_id,
                disbursementdate,
                installment,
                DUE_DATE,
                source_name, -- Include source_name
                COUNT(DISTINCT transaction_date) npayments, -- Number of distinct payment transactions
                MAX(TOTAL_DISBURSMENT_AMOUNT) AS disbursed_amount, -- Total amount disbursed for the installment
                LEAST(SUM(amount), MAX(TOTAL_DISBURSMENT_AMOUNT)) repaid_amount -- Total amount repaid, capped at disbursed amount
            FROM sub_1
            GROUP BY LN, loan_id, disbursementdate, installment, DUE_DATE, source_name -- Group by source_name
        ),

        -- Calculate FDR metrics by period and source_name
        output_1 AS (
            SELECT
                TO_CHAR(DATE_TRUNC('{{Scale}}', disbursementdate), 'YYYY-MM-DD') period, -- Truncate disbursement date
                source_name, -- Include source_name
                SUM(disbursed_amount) - SUM(repaid_amount) unpaid, -- Unpaid amount
                SUM(disbursed_amount) disbursed, -- Total disbursed amount
                (1 - SUM(repaid_amount) / SUM(disbursed_amount)) * 100 AS fin_default, -- Financial default rate
                COUNT(DISTINCT loan_id) nloans -- Number of unique loans
            FROM sub_2
            WHERE DATE(disbursementdate) > '2024-10-01' -- Filter for disbursements after a specific date
            GROUP BY 1, 2 -- Group by period and source_name
            ORDER BY 1, 2
        )
    SELECT * FROM output_1
),

-- Count successful KYC verifications by date and source name
kyc_verified AS (
    SELECT
        date_,
        source_name,
        COUNT(*) AS KYC_VERIFIED
    FROM (
        SELECT DISTINCT
            TO_CHAR(DATE_TRUNC('{{Scale}}', DATE(TIMESTAMP)), 'YYYY-MM-DD') AS date_, -- Truncate timestamp
            bn.USER_IDENTITY,
            bn.*, -- Select all columns from backend_notifications
            source_name
        FROM GHANA_PROD.DATA.BACKEND_NOTIFICATIONS bn
        JOIN ussd_data u ON u.id = bn.USER_IDENTITY -- Join with USSD signup data
        JOIN params p ON 1 = 1 -- Join with parameters CTE
        WHERE bn.TYPE = 'BE_KYC_VERIFICATION_RESULT' -- Filter for KYC verification results
          AND PARSE_JSON(payload):status = 'succeeded' -- Filter for successful verifications
          AND PARSE_JSON(payload):is_duplicate = false -- Exclude duplicate verifications
          AND DATE(TIMESTAMP) BETWEEN p.start_date AND p.end_date
    ) ml
    GROUP BY date_, source_name
    ORDER BY date_
),

-- Calculate the ratio of duplicate KYC users by date and source name
Duplicat_Users_Ratio AS (
    SELECT
        TO_CHAR(DATE_TRUNC('{{Scale}}', DATE(TIMESTAMP)), 'YYYY-MM-DD') AS date_, -- Truncate timestamp
        source_name,
        COUNT(DISTINCT(CASE WHEN PARSE_JSON(payload):is_duplicate = true THEN USER_IDENTITY END)) duplicate_users, -- Count distinct duplicate users
        COUNT(DISTINCT USER_IDENTITY) all_kyc, -- Count all distinct KYC users
        duplicate_users / all_kyc Duplicat_Users_Ratio -- Calculate ratio
    FROM (
        SELECT
            bn.*, -- Select all columns from backend_notifications
            source_name
        FROM GHANA_PROD.DATA.BACKEND_NOTIFICATIONS bn
        JOIN ussd_data u ON u.id = bn.USER_IDENTITY -- Join with USSD signup data
        JOIN params p ON 1 = 1 -- Join with parameters CTE
        WHERE bn.TYPE = 'BE_KYC_VERIFICATION_RESULT' -- Filter for KYC verification results
          AND PARSE_JSON(payload):status = 'succeeded' -- Filter for successful verifications
          AND DATE(TIMESTAMP) BETWEEN p.start_date AND p.end_date
    ) x
    GROUP BY date_, source_name
    ORDER BY date_
),

-- Count sign-ups by date and source name (re-using ussd_data CTE)
sign_ups AS (
    SELECT
        DATE_TRUNC('{{Scale}}', signup_created_date) AS date_, -- Truncate signup date
        source_name,
        COUNT(*) AS sign_ups
    FROM ussd_data -- Use the pre-filtered and ranked USSD signup data
    GROUP BY date_, source_name
    ORDER BY date_
),

-- Count clients with a first Fido score >= 250 and no fraud type matched, by date and source name
first_fs_above_250 AS (
    SELECT
        date_,
        source_name,
        COUNT(*) AS fs_eligible
    FROM (
        SELECT DISTINCT
            c.client_id,
            TO_CHAR(DATE_TRUNC('{{Scale}}', survey_date), 'YYYY-MM-DD') date_, -- Truncate survey date
            source_name
        FROM GHANA_PROD.ML.client_info c
        JOIN ussd_data u ON u.client_id = c.client_id -- Join with USSD signup data
        JOIN params p ON 1 = 1 -- Join with parameters CTE
        WHERE DATE(survey_date) BETWEEN p.start_date AND p.end_date
          AND first_fido_score >= 250 -- Filter for Fido score >= 250
          AND FRAUD_TYPE_MATCHED = '' -- Filter for no fraud type matched
          AND (PERSONAL_BR_DECISION IS NULL OR PERSONAL_BR_DECISION = 'APPROVED') -- Filter for approved or null BR decision
    )
    GROUP BY date_, source_name
    ORDER BY date_
),

-- Count clients eligible but blocked due to fraud type matched, by date and source name
Eligable_Blocked AS (
    SELECT
        date_,
        source_name,
        COUNT(*) AS Eligable_Blocked
    FROM (
        SELECT DISTINCT
            c.client_id,
            TO_CHAR(DATE_TRUNC('{{Scale}}', survey_date), 'YYYY-MM-DD') date_, -- Truncate survey date
            source_name
        FROM GHANA_PROD.ML.client_info c
        JOIN ussd_data u ON u.client_id = c.client_id -- Join with USSD signup data
        JOIN params p ON 1 = 1 -- Join with parameters CTE
        WHERE DATE(survey_date) BETWEEN p.start_date AND p.end_date
          AND first_fido_score >= 250 -- Filter for Fido score >= 250
          AND FRAUD_TYPE_MATCHED != '' -- Filter for fraud type matched
          AND PERSONAL_BR_DECISION IS NOT NULL -- Filter for non-null BR decision
    )
    GROUP BY date_, source_name
    ORDER BY date_
),

-- Count clients who finished the Fido score survey by date and source name
fs_finished AS (
    SELECT
        date_,
        source_name,
        COUNT(*) AS fs_finished
    FROM (
        SELECT DISTINCT
            c.client_id,
            TO_CHAR(DATE_TRUNC('{{Scale}}', survey_date), 'YYYY-MM-DD') date_, -- Truncate survey date
            source_name
        FROM GHANA_PROD.ML.client_info c
        JOIN ussd_data u ON u.client_id = c.client_id -- Join with USSD signup data
        JOIN params p ON 1 = 1 -- Join with parameters CTE
        WHERE DATE(survey_date) BETWEEN p.start_date AND p.end_date
    )
    GROUP BY date_, source_name
    ORDER BY date_
),

-- Count FidoBiz and Partnership disbursements by client type (New, Migrated-New, Migrated-Old), date, and source name
fidobiz AS (
    SELECT
        TO_CHAR(DATE_TRUNC('{{Scale}}', DISBURSEMENTDATE), 'YYYY-MM-DD') date_, -- Truncate disbursement date
        source_name,
        COUNT(DISTINCT CASE WHEN ln = 0 AND LOAN_PRODUCT_ID LIKE '%UCBLL%' THEN ml.CLIENT_ID ELSE NULL END) AS Fidobiz_New, -- New FidoBiz clients (LN=0)
        COUNT(DISTINCT CASE WHEN ln BETWEEN 1 AND 3 AND LOAN_PRODUCT_ID LIKE '%UCBLL%' THEN ml.CLIENT_ID ELSE NULL END) AS Fidobiz_Migrated_New, -- Migrated New FidoBiz clients (LN 1-3)
        COUNT(DISTINCT CASE WHEN ln > 3 AND LOAN_PRODUCT_ID LIKE '%UCBLL%' THEN ml.CLIENT_ID ELSE NULL END) AS Fidobiz_Migrated_old, -- Migrated Old FidoBiz clients (LN > 3)
        COUNT(DISTINCT CASE WHEN ln = 0 AND (LOAN_PRODUCT_ID LIKE '%TRSP%' OR LOAN_PRODUCT_ID LIKE '%SCTF%' OR LOAN_PRODUCT_ID LIKE '%UCBPLL%') THEN ml.CLIENT_ID ELSE NULL END) AS Partherships_New, -- New Partnership clients (LN=0)
        COUNT(DISTINCT CASE WHEN ln BETWEEN 1 AND 3 AND (LOAN_PRODUCT_ID LIKE '%TRSP%' OR LOAN_PRODUCT_ID LIKE '%SCTF%' OR LOAN_PRODUCT_ID LIKE '%UCBPLL%') THEN ml.CLIENT_ID ELSE NULL END) AS Partherships_Migrated_New, -- Migrated New Partnership clients (LN 1-3)
        COUNT(DISTINCT CASE WHEN ln > 3 AND (LOAN_PRODUCT_ID LIKE '%TRSP%' OR LOAN_PRODUCT_ID LIKE '%SCTF%' OR LOAN_PRODUCT_ID LIKE '%UCBPLL%') THEN ml.CLIENT_ID ELSE NULL END) AS Partherships_Migrated_old -- Migrated Old Partnership clients (LN > 3)
    FROM (
        SELECT
            CLIENT_KEY,
            CLIENT_ID,
            creationdate,
            DISBURSEMENTDATE,
            LN,
            LOAN_PRODUCT_ID,
            CASE
                WHEN LOAN_PRODUCT_ID LIKE '%MAL%' THEN 'Momo_Agents_POC'
                WHEN LOAN_PRODUCT_ID LIKE '%UCBLL%' THEN 'FidoBiz'
                WHEN (LOAN_PRODUCT_ID LIKE '%TRSP%' OR LOAN_PRODUCT_ID LIKE '%UCBPLL%') THEN 'Bolt'
                WHEN LOAN_PRODUCT_ID LIKE '%SCTF%' THEN 'Unilever'
            END AS product_type, -- Categorize product type
            LAG(IFF(DISBURSEMENTDATE IS NOT NULL, 1, 0), 1, 0) OVER (PARTITION BY CLIENT_ID ORDER BY creationdate ASC) AS prev_disbursed, -- Check if a previous loan was disbursed
            CONDITIONAL_TRUE_EVENT(prev_disbursed = 1) OVER (PARTITION BY CLIENT_ID ORDER BY creationdate ASC) AS new_ln -- Identify the first loan sequence
        FROM GHANA_PROD.ml.LOAN_INFO_TBL
        WHERE CASE
                WHEN LOAN_PRODUCT_ID LIKE '%MAL%' THEN 'Momo_Agents_POC'
                WHEN LOAN_PRODUCT_ID LIKE '%UCBLL%' THEN 'FidoBiz'
                WHEN (LOAN_PRODUCT_ID LIKE '%TRSP%' OR LOAN_PRODUCT_ID LIKE '%UCBPLL%') THEN 'Bolt'
                WHEN LOAN_PRODUCT_ID LIKE '%SCTF%' THEN 'Unilever'
            END IN ('Momo_Agents_POC', 'FidoBiz', 'Bolt', 'Unilever') -- Filter for specific product types
    ) ml
    JOIN params p ON 1 = 1 -- Join with parameters CTE
    JOIN ussd_data u ON u.client_id = ml.client_id -- Join with USSD signup data
    WHERE DATE(DISBURSEMENTDATE) BETWEEN p.start_date AND p.end_date
      AND new_ln = 0 -- Filter for the first loan sequence
    GROUP BY 1, 2
),

-- Count KYB (Know Your Business) submissions for new clients by date and source name
kyb AS (
    SELECT
        TO_CHAR(DATE_TRUNC('{{Scale}}', timestamp::DATE), 'YYYY-MM-DD') date_, -- Truncate timestamp
        source_name,
        COUNT(DISTINCT USER_IDENTITY) AS KYB_submitted -- Count distinct users who submitted KYB
    FROM (
        SELECT
            *,
            CASE
                WHEN (disbursementdate IS NULL OR ln = 0) THEN 'New' -- Classify as New client
                WHEN ln BETWEEN 1 AND 3 THEN 'Migrated-New' -- Classify as Migrated-New
                WHEN ln > 3 THEN 'Migrated-Old' -- Classify as Migrated-Old
            END AS Client_Type_1
        FROM (
            SELECT
                bn.*,
                PARSE_JSON(payload):application_info:application_version::STRING AS app_version -- Extract app version from payload
            FROM GHANA_PROD.DATA.BACKEND_NOTIFICATIONS bn
            JOIN ussd_data u ON u.id = bn.USER_IDENTITY -- Join with USSD signup data
            WHERE TYPE = 'BE_FIDOBIZ_SURVEY_SUBMISSION' -- Filter for FidoBiz survey submissions
            QUALIFY ROW_NUMBER() OVER (PARTITION BY bn.USER_IDENTITY ORDER BY timestamp) = 1 -- Select the first submission per user
        ) app
        LEFT JOIN GHANA_PROD.BANKING_SERVICE.USER_BANK_USER s ON app.USER_IDENTITY = s.USER_ID -- Join with user banking info
        LEFT JOIN (
            SELECT
                ml.client_id,
                disbursementdate,
                LN,
                source_name
            FROM GHANA_PROD.ml.LOAN_INFO_TBL ml
            LEFT JOIN ussd_data u ON u.client_id = ml.client_id -- Join with USSD signup data
            WHERE ml.disbursementdate IS NOT NULL
              AND ml.LOAN_PRODUCT_ID NOT LIKE '%UCBLL%' -- Exclude FidoBiz loans
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ml.CLIENT_ID ORDER BY DISBURSEMENTDATE DESC) = 1 -- Select the latest non-FidoBiz disbursement per client
        ) ml ON ml.CLIENT_ID = s.BANKING_PLATFORM_ID -- Join with loan info
        JOIN params p ON 1 = 1 -- Join with parameters CTE
        WHERE DATE(app.timestamp) BETWEEN p.start_date AND p.end_date
    )
    WHERE Client_Type_1 = 'New' -- Filter for New clients
    GROUP BY 1, 2
),

-- Count FidoBiz business document submissions in review for new clients by date and source name
biz_doc AS (
    SELECT
        TO_CHAR(DATE_TRUNC('{{Scale}}', timestamp::DATE), 'YYYY-MM-DD') date_, -- Truncate timestamp
        source_name,
        COUNT(DISTINCT USER_IDENTITY) AS biz_doc -- Count distinct users with biz doc in review
    FROM (
        SELECT
            *,
            CASE
                WHEN (disbursementdate IS NULL OR ln = 0) THEN 'New' -- Classify as New client
                WHEN ln BETWEEN 1 AND 3 THEN 'Migrated-New' -- Classify as Migrated-New
                WHEN ln > 3 THEN 'Migrated-Old' -- Classify as Migrated-Old
            END AS Client_Type_1
        FROM (
            SELECT
                bn.*,
                PARSE_JSON(payload):updated_status::STRING AS doc_status, -- Extract document status from payload
                source_name
            FROM GHANA_PROD.DATA.BACKEND_NOTIFICATIONS bn
            JOIN ussd_data u ON u.id = bn.USER_IDENTITY -- Join with USSD signup data
            WHERE TYPE = 'BE_FIDOBIZ_SCORE_DOCUMENT_STATUS_EVENT' -- Filter for FidoBiz document status events
              AND doc_status = 'IN_REVIEW' -- Filter for documents in review
            QUALIFY ROW_NUMBER() OVER (PARTITION BY bn.USER_IDENTITY ORDER BY timestamp) = 1 -- Select the first event per user
        ) app
        LEFT JOIN GHANA_PROD.BANKING_SERVICE.USER_BANK_USER s ON app.USER_IDENTITY = s.USER_ID -- Join with user banking info
        LEFT JOIN (
            SELECT
                client_id,
                disbursementdate,
                LN
            FROM GHANA_PROD.ml.LOAN_INFO_TBL ml
            WHERE ml.disbursementdate IS NOT NULL
              AND ml.LOAN_PRODUCT_ID NOT LIKE '%UCBLL%' -- Exclude FidoBiz loans
            QUALIFY ROW_NUMBER() OVER (PARTITION BY CLIENT_ID ORDER BY DISBURSEMENTDATE DESC) = 1 -- Select the latest non-FidoBiz disbursement per client
        ) ml ON ml.CLIENT_ID = s.BANKING_PLATFORM_ID -- Join with loan info
        JOIN params p ON 1 = 1 -- Join with parameters CTE
        WHERE DATE(app.timestamp) BETWEEN p.start_date AND p.end_date
    )
    WHERE Client_Type_1 = 'New' -- Filter for New clients
    GROUP BY 1, 2
),


-- Select distinct USSD clicks/opt-ins by date and source (re-using logic from ussd CTE)
clicks AS (
    SELECT DISTINCT
        TO_CHAR(DATE_TRUNC('{{Scale}}', ussd_created_date), 'YYYY-MM-DD') date_, source_name, count(*) num_of_clicks
    FROM (
        SELECT DISTINCT
            REPLACE(sl.PHONE_NUMBER, ' ','') AS PHONENUMBER, -- Clean phone number
            CASE
                    WHEN sl.SOURCE LIKE '%998*6%' THEN 'AirtelTigo'
                    WHEN sl.SOURCE LIKE '%998*77%' THEN 'BTL Activations'
                    WHEN sl.SOURCE LIKE '%998*7%' THEN 'TV'
                    WHEN sl.SOURCE LIKE '%998*99%' THEN 'Billboard'
                    WHEN sl.SOURCE LIKE '%998*8%' THEN 'MTN (Recharge Notifications)'
                    WHEN sl.SOURCE LIKE '%998*9%' THEN 'Radio'
                    WHEN sl.SOURCE LIKE '%998*11%' THEN 'Car Stickers'
                    WHEN sl.SOURCE LIKE '%998*44%' THEN 'MTN (Balance Check)'
                    WHEN sl.SOURCE LIKE '%998*55%' THEN 'Posters'
                    WHEN sl.SOURCE LIKE '%998*02%' THEN 'Delay'
                    WHEN sl.SOURCE LIKE '%998*01%' THEN 'Kwame Eugene'
                    WHEN sl.SOURCE LIKE '%998*5%' THEN 'Africa Talking TSMS'
                ELSE 'Unknown'
            END AS source_name, -- Categorize source
            sl.SOURCE, -- Original source string
            DATE(sl.CREATED_TIMESTAMP) AS ussd_created_date, -- USSD event date
            id AS ussd_id
        FROM GHANA_PROD.BANKING_SERVICE.SUBSCRIPTION_LOG sl, params
        WHERE ACTION = 'opt_in'
          AND (DATE(created_timestamp) BETWEEN start_date AND end_date)
          AND source_name <> 'Unknown' -- Exclude unknown sources
    ) s
    WHERE source_name IN ({{USSD Source Name}}) -- Filter by specified USSD sources
    group by 1,2
)

-- Final select statement: Join all relevant CTEs to produce the final report
SELECT
    total_unique_attempts.date_, 
    COALESCE(total_unique_attempts.source_name, sign_ups.source_name, kyc_verified.source_name, first_fs_above_250.source_name, kyb.source_name, biz_doc.source_name, total_unique_disbursements_ln0.source_name) AS source_name, -- Coalesce source_name from available CTEs
    clicks.num_of_clicks,
    sign_ups.sign_ups, 
    kyc_verified.kyc_verified,
    first_fs_above_250.fs_eligible,
    KYB_submitted, 
    biz_doc, 
    Personal_disbursements_ln0, 
    FidoBiz_disbursements_ln0, 
    -- fdr.fin_default
FROM total_unique_attempts
FULL JOIN total_unique_attempts_ln0 ON total_unique_attempts.date_ = total_unique_attempts_ln0.date_ AND total_unique_attempts.source_name = total_unique_attempts_ln0.source_name -- Join with LN=0 attempts
FULL JOIN sign_ups ON sign_ups.date_ = total_unique_attempts.date_ AND sign_ups.source_name = total_unique_attempts.source_name -- Join with sign-ups
FULL JOIN kyc_verified ON kyc_verified.date_ = total_unique_attempts.date_ AND kyc_verified.source_name = total_unique_attempts.source_name -- Join with KYC verified
FULL JOIN first_fs_above_250 ON first_fs_above_250.date_ = total_unique_attempts.date_ AND first_fs_above_250.source_name = total_unique_attempts.source_name -- Join with FS >= 250 eligible
FULL JOIN kyb ON kyb.date_ = total_unique_attempts.date_ AND kyb.source_name = total_unique_attempts.source_name -- Join with KYB submissions
FULL JOIN biz_doc ON biz_doc.date_ = total_unique_attempts.date_ AND biz_doc.source_name = total_unique_attempts.source_name -- Join with business document submissions
FULL JOIN clicks ON clicks.date_ = total_unique_attempts.date_ AND clicks.source_name = total_unique_attempts.source_name -- Join with business document submissions
FULL JOIN fdr ON fdr.period = total_unique_attempts.date_ and fdr.source_name = total_unique_attempts.source_name -- Join with FDR data
FULL JOIN total_unique_disbursements_ln0 ON total_unique_disbursements_ln0.date_ = total_unique_attempts.date_ AND total_unique_disbursements_ln0.source_name = total_unique_attempts.source_name -- Join with LN=0 disbursements
WHERE total_unique_attempts.date_ IS NOT NULL -- Ensure the base date from total_unique_attempts is not null
ORDER BY 1,2
