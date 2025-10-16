-- Aggregation Query: Funnel Metrics by Date, Loan Type, and Source Channel
WITH USSD_data_agg AS (
SELECT REPLACE(sl.PHONE_NUMBER, ' ','') AS PHONENUMBER, sl.SOURCE, 
       CASE WHEN sl.SOURCE like '%998*6%' THEN 'AirtelTigo'
            WHEN sl.SOURCE like '%998*77%' THEN 'BTL Activations'
	        WHEN sl.SOURCE like '%998*7%' THEN 'TV'
	        WHEN sl.SOURCE like '%998*99%' THEN 'Billboard'
	        WHEN sl.SOURCE like '%998*8%' THEN 'MTN (Recharge Notifications)'
	        WHEN sl.SOURCE like '%998*9%' THEN 'Radio'
	        WHEN sl.SOURCE like '%998*11%' THEN 'Car Stickers'
	        WHEN sl.SOURCE like '%998*44%' THEN 'MTN (Balance Check)'
	        WHEN sl.SOURCE like '%998*55%' THEN 'Posters'
	        WHEN sl.SOURCE like '%998*02%' THEN 'Delay'
	        WHEN sl.SOURCE like '%998*01%' THEN 'Kwame Eugene'
	        WHEN sl.SOURCE like '%998*5%' THEN 'Africa Talking TSMS'
	        ELSE 'Unknown' END AS source_name,
        sl.CREATED_TIMESTAMP
FROM GHANA_PROD.BANKING_SERVICE.SUBSCRIPTION_LOG sl
WHERE ACTION ='opt_in'
AND nvl(sl.SOURCE,'null') NOT IN ('null','sign_up')
),

Signup_agg AS (
SELECT DISTINCT DATE(CREATED_TIMESTAMP) AS Signup_Date, 
ID AS Signup_Id , BANKING_PLATFORM_ID, 
REPLACE(PHONE_NUMBER, ' ','') AS PHONENUMBER
from GHANA_PROD.banking_service.user 
),

ad_signups_agg AS (
SELECT ks.user_id,
      ks.USER_ID_Clean,
      bs.id,
      ks.install_date_adjusted,
      bs.banking_platform_id,
      bs.created_timestamp as signup_timestamp,
      ks.install_date_adjusted as interaction_timestamp
FROM GHANA_PROD.BANKING_SERVICE.USER bs
LEFT JOIN
  (SELECT parse_json(IDENTITY_LINK):"fido user id" AS user_id,
                                    substr (user_id,1,length(user_id)) AS USER_ID_Clean,
                                    install_date_adjusted
  FROM KOCHAVA_DATA.INSTALLS s
  LEFT JOIN kochava_data.cost_details_view c ON REGEXP_SUBSTR(s.ATTRIBUTION_CREATIVE, '\\d+') =c.creative_id) ks
ON ks.USER_ID_Clean=bs.id
where ks.USER_ID_Clean is not null
and bs.banking_platform_id is not null
),

last_interaction_channel_agg AS (
SELECT 
    s.PHONENUMBER,
    s.Signup_Id,
    s.BANKING_PLATFORM_ID,
    s.Signup_Date,
    CASE 
        WHEN COALESCE(ad.interaction_timestamp, '1900-01-01') > COALESCE(ussd.CREATED_TIMESTAMP, '1900-01-01') 
        THEN 'Ad'
        WHEN COALESCE(ussd.CREATED_TIMESTAMP, '1900-01-01') > COALESCE(ad.interaction_timestamp, '1900-01-01')
        THEN CASE 
            WHEN ussd.source_name = 'Radio' THEN 'Radio'
            ELSE 'USSD_Other'
        END
        ELSE 'Organic'
    END AS last_channel,
    CASE 
        WHEN COALESCE(ad.interaction_timestamp, '1900-01-01') > COALESCE(ussd.CREATED_TIMESTAMP, '1900-01-01') 
        THEN ad.interaction_timestamp
        WHEN COALESCE(ussd.CREATED_TIMESTAMP, '1900-01-01') > COALESCE(ad.interaction_timestamp, '1900-01-01')
        THEN ussd.CREATED_TIMESTAMP
        ELSE NULL
    END AS last_interaction_timestamp,
    COALESCE(ad.interaction_timestamp, '1900-01-01') AS ad_timestamp,
    COALESCE(ussd.CREATED_TIMESTAMP, '1900-01-01') AS ussd_timestamp
FROM Signup_agg s
LEFT JOIN ad_signups_agg ad ON s.Signup_Id = ad.USER_ID_Clean
LEFT JOIN USSD_data_agg ussd ON s.PHONENUMBER = ussd.PHONENUMBER 
    AND ussd.CREATED_TIMESTAMP < s.Signup_Date
QUALIFY ROW_NUMBER() OVER(PARTITION BY s.PHONENUMBER ORDER BY 
    GREATEST(COALESCE(ad.interaction_timestamp, '1900-01-01'), 
             COALESCE(ussd.CREATED_TIMESTAMP, '1900-01-01')) DESC) = 1
),

personal_ln0_agg AS (
SELECT u.PHONENUMBER, ml.CLIENT_ID, u.last_channel AS SOURCE, 
       CASE WHEN u.last_channel = 'Ad' THEN 'Ad'
            WHEN u.last_channel = 'USSD' THEN 'USSD'
            ELSE 'Unknown' END AS SOURCE_NAME,
       DISBURSEMENTDATE AS Ln0_Disbursementdate 
from GHANA_PROD.ml.loan_info_tbl ml 
join GHANA_PROD.mambu.client c 
on ml.client_id = c.id
join last_interaction_channel_agg u 
ON c.MOBILEPHONE1 = u.PHONENUMBER
WHERE LN = 0 
AND IS_DISBURSED = TRUE 
AND LOAN_PRODUCT_ID LIKE 'UCLL%'
and u.last_interaction_timestamp < ml.DISBURSEMENTDATE
),

Fidobiz_agg AS (
SELECT u.PHONENUMBER, u.last_channel AS SOURCE, 
       CASE WHEN u.last_channel = 'Ad' THEN 'Ad'
            WHEN u.last_channel = 'USSD' THEN 'USSD'
            ELSE 'Unknown' END AS SOURCE_NAME,
       ml.CLIENT_ID, Disbursementdate as Fidobiz_Disbursementdate, c.MOBILEPHONE1,
CASE WHEN LN = 0 THEN 'New'
    WHEN LN BETWEEN 1 AND 3 THEN 'Migrated-New'
    WHEN LN > 3 THEN 'Migrated-Old' END AS Fidobiz_Status , 
    LN, LOAN_PRODUCT_ID, ml.CREATIONDATE, LAG(IFF(DISBURSEMENTDATE is not null,1,0),1,0) over (PARTITION BY CLIENT_ID ORDER BY ml.creationdate  ASC) as prev_disbursed,
	       CONDITIONAL_TRUE_EVENT(prev_disbursed=1) OVER (PARTITION BY CLIENT_ID ORDER BY ml.creationdate ASC) as new_ln
	FROM GHANA_PROD.ml.LOAN_INFO_TBL ml 
    JOIN GHANA_PROD.MAMBU.CLIENT c 
    ON ml.CLIENT_ID = c.ID
    JOIN last_interaction_channel_agg u
    ON c.MOBILEPHONE1 = u.PHONENUMBER
    WHERE LOAN_PRODUCT_ID LIKE 'UCBLL%'
    AND DISBURSEMENTDATE IS NOT NULL
    AND u.last_interaction_timestamp < ml.disbursementdate
),

Fidobiz_L0_agg AS (
SELECT * FROM Fidobiz_agg 
where new_ln = 0
),

funnel_metrics AS (
    -- Last Interaction step
    SELECT 
        DATE(lic.Signup_Date) as funnel_date,
        'Last Interaction' as loan_type,
        lic.last_channel as source,
        CASE 
            WHEN lic.last_channel = 'Ad' THEN 'Ad'
            WHEN lic.last_channel = 'Radio' THEN 'Radio'
            WHEN lic.last_channel = 'USSD_Other' THEN 'USSD_Other'
            WHEN lic.last_channel = 'Organic' THEN 'Organic'
            ELSE 'Unknown'
        END as source_name,
        COUNT(DISTINCT lic.PHONENUMBER) as user_count
    FROM last_interaction_channel_agg lic
    GROUP BY 1, 2, 3, 4
    
    UNION ALL
    
    -- Signup step
    SELECT 
        DATE(lic.Signup_Date) as funnel_date,
        'Signup' as loan_type,
        lic.last_channel as source,
        CASE 
            WHEN lic.last_channel = 'Ad' THEN 'Ad'
            WHEN lic.last_channel = 'Radio' THEN 'Radio'
            WHEN lic.last_channel = 'USSD_Other' THEN 'USSD_Other'
            WHEN lic.last_channel = 'Organic' THEN 'Organic'
            ELSE 'Unknown'
        END as source_name,
        COUNT(DISTINCT lic.PHONENUMBER) as user_count
    FROM last_interaction_channel_agg lic
    GROUP BY 1, 2, 3, 4
    
    UNION ALL
    
    -- Personal Loan step
    SELECT 
        DATE(lic.Signup_Date) as funnel_date,
        'Personal_L0' as loan_type,
        lic.last_channel as source,
        CASE 
            WHEN lic.last_channel = 'Ad' THEN 'Ad'
            WHEN lic.last_channel = 'Radio' THEN 'Radio'
            WHEN lic.last_channel = 'USSD_Other' THEN 'USSD_Other'
            WHEN lic.last_channel = 'Organic' THEN 'Organic'
            ELSE 'Unknown'
        END as source_name,
        COUNT(DISTINCT lic.PHONENUMBER) as user_count
    FROM last_interaction_channel_agg lic
    INNER JOIN personal_ln0_agg pl ON lic.PHONENUMBER = pl.PHONENUMBER
    GROUP BY 1, 2, 3, 4
    
    UNION ALL
    
    -- Fidobiz steps
    SELECT 
        DATE(lic.Signup_Date) as funnel_date,
        CASE 
            WHEN f.Fidobiz_Status = 'New' THEN 'Fidobiz_New'
            WHEN f.Fidobiz_Status = 'Migrated-New' THEN 'Fidobiz_Migrated_New'
            WHEN f.Fidobiz_Status = 'Migrated-Old' THEN 'Fidobiz_Migrated_Old'
        END as loan_type,
        lic.last_channel as source,
        CASE 
            WHEN lic.last_channel = 'Ad' THEN 'Ad'
            WHEN lic.last_channel = 'Radio' THEN 'Radio'
            WHEN lic.last_channel = 'USSD_Other' THEN 'USSD_Other'
            WHEN lic.last_channel = 'Organic' THEN 'Organic'
            ELSE 'Unknown'
        END as source_name,
        COUNT(DISTINCT lic.PHONENUMBER) as user_count
    FROM last_interaction_channel_agg lic
    INNER JOIN Fidobiz_L0_agg f ON lic.PHONENUMBER = f.PHONENUMBER
    GROUP BY 1, 2, 3, 4
    
    UNION ALL
    
    -- Include unattributed users (users who signed up but never got loans)
    SELECT 
        DATE(s.Signup_Date) as funnel_date,
        'Signup_Only' as loan_type,
        COALESCE(lic.last_channel, 'Unattributed') as source,
        CASE 
            WHEN lic.last_channel = 'Ad' THEN 'Ad'
            WHEN lic.last_channel = 'Radio' THEN 'Radio'
            WHEN lic.last_channel = 'USSD_Other' THEN 'USSD_Other'
            WHEN lic.last_channel = 'Organic' THEN 'Organic'
            WHEN lic.last_channel IS NULL THEN 'Unattributed'
            ELSE 'Unknown'
        END as source_name,
        COUNT(DISTINCT s.PHONENUMBER) as user_count
    FROM Signup_agg s
    LEFT JOIN last_interaction_channel_agg lic ON s.PHONENUMBER = lic.PHONENUMBER
    WHERE s.PHONENUMBER NOT IN (
        SELECT DISTINCT PHONENUMBER FROM personal_ln0_agg
        UNION
        SELECT DISTINCT PHONENUMBER FROM Fidobiz_L0_agg
    )
    GROUP BY 1, 2, 3, 4
),

daily_summary AS (
    SELECT 
        funnel_date,
        source,
        source_name,
        SUM(CASE WHEN loan_type = 'Last Interaction' THEN user_count ELSE 0 END) as last_interaction_users,
        SUM(CASE WHEN loan_type = 'Signup' THEN user_count ELSE 0 END) as signup_users,
        SUM(CASE WHEN loan_type = 'Personal_L0' THEN user_count ELSE 0 END) as personal_loan_users,
        SUM(CASE WHEN loan_type = 'Fidobiz_New' THEN user_count ELSE 0 END) as fidobiz_new_users,
        SUM(CASE WHEN loan_type = 'Fidobiz_Migrated_New' THEN user_count ELSE 0 END) as fidobiz_migrated_new_users,
        SUM(CASE WHEN loan_type = 'Fidobiz_Migrated_Old' THEN user_count ELSE 0 END) as fidobiz_migrated_old_users,
        SUM(CASE WHEN loan_type = 'Signup_Only' THEN user_count ELSE 0 END) as signup_only_users
    FROM funnel_metrics
    GROUP BY 1, 2, 3
)

SELECT 
    funnel_date,
    source,
    source_name,
    last_interaction_users,
    signup_users,
    personal_loan_users,
    fidobiz_new_users,
    fidobiz_migrated_new_users,
    fidobiz_migrated_old_users,
    signup_only_users,
    -- Conversion rates
    ROUND(signup_users * 100.0 / NULLIF(last_interaction_users, 0), 2) as signup_rate_pct,
    ROUND(personal_loan_users * 100.0 / NULLIF(signup_users, 0), 2) as personal_loan_rate_pct,
    ROUND(fidobiz_new_users * 100.0 / NULLIF(signup_users, 0), 2) as fidobiz_new_rate_pct,
    ROUND(fidobiz_migrated_new_users * 100.0 / NULLIF(signup_users, 0), 2) as fidobiz_migrated_new_rate_pct,
    ROUND(fidobiz_migrated_old_users * 100.0 / NULLIF(signup_users, 0), 2) as fidobiz_migrated_old_rate_pct,
    -- Total funnel users
    (last_interaction_users + signup_only_users) as total_traffic_users
FROM daily_summary
ORDER BY funnel_date DESC, source;