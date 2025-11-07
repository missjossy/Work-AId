WITH
        -- Select distinct sign-ups within August and September 2025
        sign_ups AS (
            SELECT DISTINCT
                DATE(CREATED_TIMESTAMP) AS signup_created_date,
                "USER".ID AS signups_id,
                REPLACE(PHONE_NUMBER, ' ','') AS PHONENUMBER, -- Clean phone number by removing spaces
                BANKING_PLATFORM_ID,
                ID
            FROM GHANA_PROD.BANKING_SERVICE."USER"
            WHERE DATE(CREATED_TIMESTAMP) BETWEEN '2025-09-01' AND '2025-09-30'
        ),

        -- Select distinct USSD opt-in events and categorize source
        -- Include USSD events from before August to capture attribution for Aug/Sep signups
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
                sl.id AS ussd_id
            FROM GHANA_PROD.BANKING_SERVICE.SUBSCRIPTION_LOG sl
            WHERE ACTION = 'opt_in'
              AND DATE(created_timestamp) <= '2025-09-30' -- Include USSD events up to end of September
        ),

        -- Join USSD events with sign-ups to find conversions and rank engagements
        ussd_sign_ups AS (
            SELECT
                s.BANKING_PLATFORM_ID client_id,
                s.ID signup_id, -- Use signup ID to uniquely identify each signup
                u.PHONENUMBER,
                s.PHONENUMBER signup_phonenumber,
                SOURCE_NAME,
                ussd_created_date,
                signup_created_date,
                DATEDIFF(DAY, USSD_CREATED_DATE, SIGNUP_CREATED_DATE) days_to_conv, -- Days from USSD event to signup
                u.ussd_id,
                ROW_NUMBER() OVER (
                    PARTITION BY s.ID 
                    ORDER BY USSD_CREATED_DATE DESC
                ) engagement_rank -- Rank USSD engagements per signup (most recent first)
            FROM sign_ups s
            JOIN ussd u ON s.PHONENUMBER = u.PHONENUMBER
                       AND USSD_CREATED_DATE <= SIGNUP_CREATED_DATE -- Ensure USSD event happened before or on signup date
        ),

        -- Get last USSD channel before each signup (engagement_rank = 1)
        last_ussd_channel AS (
            SELECT
                client_id,
                signup_id,
                PHONENUMBER,
                SOURCE_NAME,
                signup_created_date,
                ussd_created_date,
                days_to_conv
            FROM ussd_sign_ups
            WHERE engagement_rank = 1
        )

-- Aggregate monthly by USSD source for August and September
SELECT
    DATE_TRUNC('MONTH', signup_created_date) AS signup_month,
    SOURCE_NAME,
    COUNT(DISTINCT signup_id) AS signup_count,
    COUNT(DISTINCT client_id) AS unique_clients,
    COUNT(DISTINCT PHONENUMBER) AS unique_phone_numbers,
    ROUND(AVG(days_to_conv), 2) AS avg_days_to_conversion,
    MIN(days_to_conv) AS min_days_to_conversion,
    MAX(days_to_conv) AS max_days_to_conversion
FROM last_ussd_channel
WHERE DATE_TRUNC('MONTH', signup_created_date) IN ('2025-08-01', '2025-09-01')
  AND SOURCE_NAME <> 'Unknown'
GROUP BY 
    DATE_TRUNC('MONTH', signup_created_date),
    SOURCE_NAME
ORDER BY 
    signup_month,
    signup_count DESC