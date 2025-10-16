
with ussd_subs as (  
SELECT sl.*, case when source = '*998*9#' then 'radio' else 'other' end as ussd_type,
          u.banking_platform_id,
          sl.created_timestamp as interaction_timestamp
   FROM banking_service.subscription_log sl
   LEFT JOIN GHANA_PROD.BANKING_SERVICE."USER" u ON right(u.PHONE_NUMBER,9) = right(sl.phone_number,9) 
   WHERE action = 'opt_in'
     AND SOURCE != 'sign_up'
     and u.banking_platform_id is not null
     and date(sl.created_timestamp) between date('2025-04-01') and date('2025-07-31')
     ),

add_signups AS (
SELECT ks.user_id,
      ks.USER_ID_Clean,
      ks.install_date_adjusted,
      bs.banking_platform_id,
      bs.created_timestamp as signup_timestamp,
      ks.install_date_adjusted as interaction_timestamp
FROM BANKING_SERVICE.USER bs
LEFT JOIN
  (SELECT parse_json(IDENTITY_LINK):"fido user id" AS user_id,
                                    substr (user_id,1,length(user_id)) AS USER_ID_Clean,
                                    install_date_adjusted
  FROM KOCHAVA_DATA.INSTALLS s
  LEFT JOIN kochava_data.cost_details_view c ON REGEXP_SUBSTR(s.ATTRIBUTION_CREATIVE, '\\d+') =c.creative_id) ks
ON ks.USER_ID_Clean=bs.id
where ks.USER_ID_Clean is not null
and bs.banking_platform_id is not null),

-- Create unified marketing interactions timeline
marketing_interactions as (
  -- Ad interactions
  SELECT 
    banking_platform_id,
    interaction_timestamp,
    'ad' as channel
  FROM add_signups
  
  UNION ALL
  
  -- USSD radio interactions
  SELECT 
    banking_platform_id,
    interaction_timestamp,
    'radio_ussd' as channel
  FROM ussd_subs 
  WHERE ussd_type = 'radio'
  
  UNION ALL
  
  -- USSD other interactions
  SELECT 
    banking_platform_id,
    interaction_timestamp,
    'other_ussd' as channel
  FROM ussd_subs 
  WHERE ussd_type = 'other'
),

-- Get last interaction for each user before signup
last_interaction as (
  SELECT 
    u.banking_platform_id,
    u.created_timestamp as signup_timestamp,
    mi.channel as last_channel,
    mi.interaction_timestamp as last_interaction_time,
    CASE 
      WHEN EXISTS (
        SELECT 1 FROM marketing_interactions mi2 
        WHERE mi2.banking_platform_id = u.banking_platform_id
      ) THEN
        CASE 
          WHEN mi.channel IS NOT NULL THEN mi.channel
          ELSE 'organic'
        END
      ELSE 'organic'
    END as sources,
    ROW_NUMBER() OVER (
      PARTITION BY u.banking_platform_id 
      ORDER BY mi.interaction_timestamp DESC
    ) as rn
  FROM BANKING_SERVICE.USER u
  LEFT JOIN marketing_interactions mi 
    ON u.banking_platform_id = mi.banking_platform_id
    AND mi.interaction_timestamp < u.created_timestamp
  WHERE date(u.created_timestamp) between date('2025-04-01') and date('2025-07-31')
    AND u.banking_platform_id is not null
)
SELECT 
  date(li.signup_timestamp) as signup_date,
  CASE 
    WHEN date(li.signup_timestamp) < date('2025-06-01') THEN 0 
    ELSE 1 
  END as radio_campaign_active,
  COUNT(DISTINCT CASE WHEN li.sources = 'organic' THEN li.banking_platform_id END) as organic_signups,
  COUNT(DISTINCT CASE WHEN li.sources = 'ad' THEN li.banking_platform_id END) as ad_signups,
  COUNT(DISTINCT CASE WHEN li.sources = 'radio_ussd' THEN li.banking_platform_id END) as radio_ussd_signups,
  COUNT(DISTINCT CASE WHEN li.sources = 'other_ussd' THEN li.banking_platform_id END) as other_ussd_signups,
  COUNT(DISTINCT li.banking_platform_id) as total_signups
FROM last_interaction li
LEFT JOIN (
  SELECT *, CASE 
    WHEN region IN ('Accra', 'Tema') THEN 'Greater Accra Region'
    WHEN region = 'Other Region' THEN cust_location|| ' '|| 'Region' 
    ELSE region 
  END as regions
  FROM data.survey_data
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CLIENT_ID ORDER BY LOAN_DATE DESC) = 1
) cl ON li.banking_platform_id = cl.client_id
WHERE (li.rn = 1 OR li.rn IS NULL) 
  AND cl.regions in ('Greater Accra Region', 'Ashanti Region', 'Western Region', 'Western North Region')
GROUP BY date(li.signup_timestamp)  
ORDER BY signup_date
