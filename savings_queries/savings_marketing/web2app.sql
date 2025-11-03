with web as (
    SELECT web_signup, PHONE AS phone_signup
    FROM GH_DEV.DATA.VW_WEBFLOW_FORM_SUBMISSIONS_CLEANED
    WHERE DISPLAYNAME_ not in ({{display_name}}) 
    and nvl(utm_source,'other') in ({{Source}})
    and nvl(campaign, 'other') not in ({{campaign_name}})
    AND date(web_signup) between date('{{start_date}}') and date('{{end_date}}') 
    and phone is not null 
),

all_web as (
    select 'web_signup' as value_1, 
           count(distinct phone_signup) as web_signup 
    from web 
),

signups as (
    select 'Signups' as value_1,
           'Kyc_Verified' as value_2,
           count(distinct id) signups, 
           count(distinct case when id is not null and KYC_VERIFIED = TRUE and user_status= 'ACTIVE' then id else null end) KYC_VERIFIED 
    from (
        select s.CREATED_TIMESTAMP::date as user_creation_date,
               parse_json(status):status as user_status,
               *
        from web as pt
        LEFT JOIN GHANA_PROD.BANKING_SERVICE."USER" s ON RIGHT(s.PHONE_NUMBER,9) = RIGHT(pt.phone_signup,9)
        where s.created_timestamp::date >= pt.web_signup::date 
    )
),

savings_account_base as (
    SELECT cl.id as client_id, 
           sa.id as account_id,
           sa.accountstate,
           sa.creationdate as first_creationdate,
           pt.web_signup,
           ROW_NUMBER() OVER(partition by cl.id order by sa.creationdate asc) as rn
    FROM web pt
    JOIN GHANA_PROD.BANKING_SERVICE."USER" s ON RIGHT(s.PHONE_NUMBER,9) = RIGHT(pt.phone_signup,9)
    JOIN MAMBU.CLIENT cl on cl.id = s.BANKING_PLATFORM_ID
    LEFT JOIN GHANA_PROD.MAMBU.SAVINGSACCOUNT sa on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
    LEFT JOIN GHANA_PROD.MAMBU.SAVINGSTRANSACTION st ON st.PARENTACCOUNTKEY = sa.ENCODEDKEY
    LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_id ON st.ENCODEDKEY = wallet_id.PARENTKEY 
        AND wallet_id.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'ACCOUNT_NUMBER_TRANSACTION_CHANN')
    LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_network ON st.ENCODEDKEY = wallet_network.PARENTKEY 
        AND wallet_network.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'NETWORK_TRANSACTION_CHANNEL')
    LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE identifier ON st.ENCODEDKEY = identifier.PARENTKEY 
        AND identifier.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'IDENTIFIER_TRANSACTION_CHANNEL_I')
    where sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
    and (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584', '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
    and (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584', '23346722591', '233257806345') or cl.MOBILEPHONE2 is null) 
    and (wallet_id.value not in ('233552602681', '233243630046', '233245822584', '23346722591', '233257806345') or wallet_id.value is null)
    QUALIFY rn = 1
),

savings_signups as (
    select 'savings_signups' as value_1, 
           count(distinct client_id) as savings_signups
    from savings_account_base
    where date(first_creationdate) >= web_signup::date
    and datediff(day, web_signup::date, first_creationdate::date) between 0 and {{convertions_days}}
),

survey as (
    SELECT 'survey_filled' as value_1,
           count(distinct case when FIRST_FIDO_SCORE is not null then client_id else null end) as survey_filled
    from (
        select *  
        from web pt
        JOIN GHANA_PROD.BANKING_SERVICE."USER" s ON RIGHT(s.PHONE_NUMBER,9) = RIGHT(pt.phone_signup,9)
        JOIN GHANA_PROD.ml.CLIENT_INFO cl on cl.client_id = s.BANKING_PLATFORM_ID
        where created_timestamp::date >= web_signup::date
        and datediff(day, web_signup::date, created_timestamp::date) between 0 and {{convertions_days}}
        QUALIFY row_number() over (partition by client_id order by SURVEY_DATE DESC) = 1
    )
)

select value_1, web_signup from all_web
union
select value_1, signups from signups
union 
select value_2, KYC_VERIFIED from signups
union 
select value_1, survey_filled from survey
union
select value_1, savings_signups from savings_signups