-- Easysave Campaign Analysis: Web2App vs Kochava Attribution
-- Shows source, specific Easysave campaigns, savings signups, and users without loans

WITH web AS (
    -- Web2App traffic (from web2app.sql structure)
    SELECT 
        web_signup, 
        PHONE AS phone_signup,
        COALESCE(campaign, 'Web2App') as campaign_name
    FROM GH_DEV.DATA.VW_WEBFLOW_FORM_SUBMISSIONS_CLEANED
    WHERE DISPLAYNAME_ not in ('Test User', 'Fido Money', 'Admin User') 
    AND (LOWER(campaign) LIKE '%easysave%' OR LOWER(utm_source) LIKE '%easysave%' OR campaign IS NULL)
    AND nvl(utm_source,'other') in ('google', 'facebook', 'instagram')
    AND date(web_signup) between date('2025-04-01') and date('2025-09-30') 
    AND phone is not null 
),

web_traffic AS (
    -- All Web2App traffic
    SELECT 
        'Web2App' as source,
        campaign_name,
        phone_signup,
        web_signup as signup_date,
        s.id as fido_user_id,
        s.BANKING_PLATFORM_ID as client_id
    FROM web pt
    LEFT JOIN GHANA_PROD.BANKING_SERVICE."USER" s ON RIGHT(s.PHONE_NUMBER,9) = RIGHT(pt.phone_signup,9)
        AND s.created_timestamp::date >= pt.web_signup::date
),

web_signups AS (
    -- Web2App users who actually signed up
    SELECT * FROM web_traffic WHERE fido_user_id IS NOT NULL
),

kochava_installs AS (
    -- Kochava attribution data
    SELECT 
        *, 
        parse_json(IDENTITY_LINK):"fido user id" as user_id,
        substr(user_id,1,length(user_id)) as USER_ID_Clean, 
        s.ATTRIBUTION_CAMPAIGN_NAME,
        REGEXP_SUBSTR(ATTRIBUTION_CREATIVE, '\\d+') AS creative_id,
        case when (ATTRIBUTION_NETWORK_NAME ='Facebook' or ATTRIBUTION_NETWORK_NAME='Instagram') 
             then SUBSTRING(ATTRIBUTION_SITE, 1, CHARINDEX('(', ATTRIBUTION_SITE) - 1)
             when ATTRIBUTION_SITE like '{"campaign_name"%' 
             then parse_json(ATTRIBUTION_SITE):"campaign_name" 
             else ATTRIBUTION_SITE 
        end as site_campaign_name,
        case when ATTRIBUTION_NETWORK_NAME ='Facebook' 
             then c.PARTNER_CAMPAIGN_ID 
             when ATTRIBUTION_NETWORK_NAME !='Facebook' and ATTRIBUTION_SITE like '{"campaign_name"%'  
             then parse_json(ATTRIBUTION_SITE):"campaignid"::varchar 
             else null 
        end as site_campaign_id,
        parse_json(INSTALL_DEVICES_IDS):kochava_device_id as kochava_device_id 
    FROM KOCHAVA_DATA.INSTALLS s
    LEFT JOIN kochava_data.cost_details_view c ON REGEXP_SUBSTR(s.ATTRIBUTION_CREATIVE, '\\d+') = c.creative_id
    WHERE install_date_UTC::date between '2025-04-01' and '2025-09-30'
),

kochava_signups AS (
    -- Kochava attributed signups (filtered for Easysave campaigns)
    SELECT 
        case when ks.ATTRIBUTION_NETWORK_NAME = 'Facebook' then 'Meta (Facebook)'
             when ks.ATTRIBUTION_NETWORK_NAME = 'Instagram' then 'Meta (Instagram)'
             when ks.ATTRIBUTION_NETWORK_NAME = 'Google' then 'Google'
             else ks.ATTRIBUTION_NETWORK_NAME 
        end as source,
        case when ks.site_campaign_name is null and ks.ATTRIBUTION_NETWORK_NAME='UNATTRIBUTED' 
             then 'UNATTRIBUTED' 
             else ks.site_campaign_name 
        end as campaign_name,
        s.id as fido_user_id,
        s.BANKING_PLATFORM_ID as client_id,
        s.CREATED_TIMESTAMP as signup_date
    FROM BANKING_SERVICE.USER s 
    LEFT JOIN kochava_installs ks ON ks.USER_ID_Clean = s.id 
        AND install_date_UTC::date <= s.CREATED_TIMESTAMP::date
    WHERE date(s.CREATED_TIMESTAMP) between '2025-04-01' and '2025-09-30'
    AND (LOWER(ks.site_campaign_name) LIKE '%easysave%' OR ks.site_campaign_name IS NULL)
    QUALIFY row_number() over (partition by s.id order by ks.install_date_UTC desc) = 1
),

all_signups AS (
    -- Combine both sources
    SELECT source, campaign_name, fido_user_id, client_id, signup_date FROM web_signups
    UNION ALL
    SELECT source, campaign_name, fido_user_id, client_id, signup_date FROM kochava_signups
),

all_traffic AS (
    -- Combine all traffic (Web2App + Kochava signups)
    SELECT source, campaign_name, fido_user_id, client_id, signup_date FROM web_traffic
    UNION ALL
    SELECT source, campaign_name, fido_user_id, client_id, signup_date FROM kochava_signups
),

savings_accounts AS (
    -- First savings account for each client
    SELECT 
        cl.id as client_id, 
        sa.id as account_id,
        sa.creationdate as first_savings_date,
        ROW_NUMBER() OVER(partition by cl.id order by sa.creationdate asc) as rn
    FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa
    LEFT JOIN MAMBU.CLIENT cl ON cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
    LEFT JOIN GHANA_PROD.MAMBU.SAVINGSTRANSACTION st ON st.PARENTACCOUNTKEY = sa.ENCODEDKEY
    LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_id ON st.ENCODEDKEY = wallet_id.PARENTKEY 
        AND wallet_id.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'ACCOUNT_NUMBER_TRANSACTION_CHANN')
    WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
    AND (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584', '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
    AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584', '23346722591', '233257806345') or cl.MOBILEPHONE2 is null) 
    AND (wallet_id.value not in ('233552602681', '233243630046', '233245822584', '23346722591', '233257806345') or wallet_id.value is null)
    AND date(sa.creationdate) >= '2025-04-01'
    QUALIFY rn = 1
),

loan_clients AS (
    -- Clients who have taken loans
    SELECT DISTINCT client_id
    FROM ML.LOAN_INFO_TBL 
    WHERE disbursementdate is not null
),

marketing_savings AS (
    -- Marketing clients with savings accounts
    SELECT 
        asu.source,
        asu.campaign_name,
        asu.client_id,
        asu.signup_date,
        sa.first_savings_date,
        case when lc.client_id is not null then 'Has Loans' else 'No Loans' end as loan_status
    FROM all_signups asu
    LEFT JOIN savings_accounts sa ON asu.client_id = sa.client_id
    LEFT JOIN loan_clients lc ON asu.client_id = lc.client_id
    WHERE sa.client_id is not null  -- Only clients with savings accounts
)

-- Final results grouped by source and campaign
SELECT 
    COALESCE(ms.source, at.source) as source,
    COALESCE(ms.campaign_name, at.campaign_name) as campaign_name,
    COUNT(DISTINCT at.client_id) as total_traffic,
    COUNT(DISTINCT ms.client_id) as total_savings_signups,
    COUNT(DISTINCT CASE WHEN ms.loan_status = 'No Loans' THEN ms.client_id END) as users_without_loans,
    COUNT(DISTINCT CASE WHEN ms.loan_status = 'Has Loans' THEN ms.client_id END) as users_with_loans,
    ROUND(COUNT(DISTINCT ms.client_id) * 100.0 / NULLIF(COUNT(DISTINCT at.client_id), 0), 2) as conversion_rate,
    ROUND(COUNT(DISTINCT CASE WHEN ms.loan_status = 'No Loans' THEN ms.client_id END) * 100.0 / NULLIF(COUNT(DISTINCT ms.client_id), 0), 2) as pct_without_loans,
    ROUND(COUNT(DISTINCT CASE WHEN ms.loan_status = 'Has Loans' THEN ms.client_id END) * 100.0 / NULLIF(COUNT(DISTINCT ms.client_id), 0), 2) as pct_with_loans
FROM all_traffic at
LEFT JOIN marketing_savings ms ON at.source = ms.source AND at.campaign_name = ms.campaign_name
GROUP BY COALESCE(ms.source, at.source), COALESCE(ms.campaign_name, at.campaign_name)
ORDER BY 
    CASE COALESCE(ms.source, at.source)
        WHEN 'Web2App' THEN 1
        WHEN 'Google' THEN 2  
        WHEN 'Meta (Facebook)' THEN 3
        WHEN 'Meta (Instagram)' THEN 4
        ELSE 5
    END,
    total_savings_signups DESC;
