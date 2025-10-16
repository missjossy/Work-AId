with installs as 
(SELECT *, parse_json(IDENTITY_LINK):"fido user id" as user_id,substr (user_id,1,length(user_id)) as USER_ID_Clean, s.ATTRIBUTION_CAMPAIGN_NAME,
REGEXP_SUBSTR(ATTRIBUTION_CREATIVE, '\\d+') AS creative_id,
case when (ATTRIBUTION_NETWORK_NAME ='Facebook'  or ATTRIBUTION_NETWORK_NAME='Instagram') then SUBSTRING(ATTRIBUTION_SITE, 1, CHARINDEX('(', ATTRIBUTION_SITE) - 1)
     when ATTRIBUTION_SITE like '{"campaign_name"%' then parse_json (ATTRIBUTION_SITE):"campaign_name" 
     else ATTRIBUTION_SITE end as site_campaign_name,
case when ATTRIBUTION_NETWORK_NAME ='Facebook'  then c.PARTNER_CAMPAIGN_ID 
     when ATTRIBUTION_NETWORK_NAME !='Facebook' and ATTRIBUTION_SITE like '{"campaign_name"%'  then parse_json (ATTRIBUTION_SITE):"campaignid"::varchar 
     else null end as site_campaign_id,
parse_json(INSTALL_DEVICES_IDS):kochava_device_id as kochava_device_id FROM KOCHAVA_DATA.INSTALLS s
left join kochava_data.cost_details_view c on  REGEXP_SUBSTR(s.ATTRIBUTION_CREATIVE, '\\d+') =c.creative_id)

,kyc_verified AS (  
    SELECT distinct date(TIMESTAMP) as date_, USER_IDENTITY
    		FROM ghana_prod.DATA.BACKEND_NOTIFICATIONS WHERE TYPE='BE_KYC_VERIFICATION_RESULT'
            AND parse_json(payload):status = 'succeeded'
            AND parse_json(payload):is_duplicate = false )
            
,cost_data AS (
SELECT date_trunc('{{ time }}',cost_date::date) as time ,network_partner_name, partner_campaign_name ,partner_campaign_id, sum(total_spend_USD) daily_cost FROM (
SELECT DISTINCT * FROM ghana_prod.kochava_data.cost_details_view)
GROUP BY 1,2,3,4)            
 
,signups as (
select date_trunc('{{ time }}',CREATED_TIMESTAMP::date) as time,ATTRIBUTION_NETWORK_NAME,
case when site_campaign_name is null and ATTRIBUTION_NETWORK_NAME='UNATTRIBUTED' then 'UNATTRIBUTED' else site_campaign_name end as site_campaign_name,
count(distinct fido_user_id) as signups,
count(distinct case when  KYC_VERIFIED =true then fido_user_id else null end) as KYC_VERIFIED
        from(
            select distinct USER_ID_Clean,ks.user_id as kochava_user_id, date(s.CREATED_TIMESTAMP) as CREATED_TIMESTAMP ,ks.kochava_device_id, 
            ks.install_date_UTC ,s.id as fido_user_id,site_campaign_id,site_campaign_name,KYC_VERIFIED,
            datediff(day,ks.install_date_UTC,s.CREATED_TIMESTAMP) days_diff  ,ATTRIBUTION_NETWORK_NAME , INSTALL_DEVICES_IP ,
            INSTALLGEO_COUNTRY_CODE3 ,ATTRIBUTION_TRACKER_NAME  ,ATTRIBUTION_CREATIVE , s.type as user_type,
            from BANKING_SERVICE.USER s 
            left join installs ks on ks.USER_ID_Clean=s.id ---and install_date_UTC::date <= s.CREATED_TIMESTAMP::date
            where date(s.CREATED_TIMESTAMP) between '{{range.start}}' and  '{{range.end}}'
            qualify row_number() over (partition by s.id order by ks.install_date_UTC desc)=1)
            group by 1,2,3)

,savings_account_base as (
    SELECT cl.id as client_id, sa.id as account_id,
    sa.accountstate,
    sa.creationdate,
    ROW_NUMBER() OVER(partition by cl.id order by sa.creationdate asc) as rn
    FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa
    LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
LEFT JOIN GHANA_PROD.MAMBU.SAVINGSTRANSACTION st ON st.PARENTACCOUNTKEY = sa.ENCODEDKEY
    LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_id ON st.ENCODEDKEY = wallet_id.PARENTKEY AND wallet_id.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'ACCOUNT_NUMBER_TRANSACTION_CHANN')
    LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_network ON st.ENCODEDKEY = wallet_network.PARENTKEY AND wallet_network.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'NETWORK_TRANSACTION_CHANNEL')
    LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE identifier ON st.ENCODEDKEY = identifier.PARENTKEY AND identifier.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'IDENTIFIER_TRANSACTION_CHANNEL_I')
    where sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
    and (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
    and (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE2 is null) 
    and (wallet_id.value not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or wallet_id.value is null)
    AND date(sa.creationdate) >= date('2025-04-03')
    QUALIFY rn = 1
)

,SAVINGS_SIGNUPS as (
    select date_trunc('{{ time }}',first_creation_date::date) as time,
           ATTRIBUTION_NETWORK_NAME,
           site_campaign_id,
           case when site_campaign_name is null and ATTRIBUTION_NETWORK_NAME='UNATTRIBUTED' then 'UNATTRIBUTED' else site_campaign_name end as site_campaign_name,
           count(distinct savings_client) as Total_Savings_Signups
    from (
        select distinct USER_ID_Clean,
               ks.user_id as kochava_user_id, 
               date(s.CREATED_TIMESTAMP) as CREATED_TIMESTAMP,
               ks.kochava_device_id, 
               ks.install_date_UTC,
               s.id as fido_user_id,
               datediff(day,ks.install_date_UTC,s.CREATED_TIMESTAMP) days_diff,
               INSTALL_DEVICES_IP,
               ATTRIBUTION_NETWORK_NAME,
               INSTALLGEO_COUNTRY_CODE3,
               ATTRIBUTION_TRACKER_NAME,
               ATTRIBUTION_CREATIVE,
               s.type as user_type,
               sab.creationdate as first_creation_date,
               site_campaign_name,
               site_campaign_id,
               sab.client_id as savings_client
        from savings_account_base sab
        left join BANKING_SERVICE.USER s on sab.client_id = s.BANKING_PLATFORM_ID
        left join installs ks on ks.USER_ID_Clean = s.id and install_date_UTC::date <= sab.creationdate::date
        where date(sab.creationdate) between '{{range.start}}' and '{{range.end}}'
        qualify row_number() over (partition by sab.client_id order by ks.install_date_UTC desc) = 1
    ) raw_data
    group by 1,2,3,4
)

,LN0_DISBURSMENTS as(
select date_trunc('{{ time }}',DISBURSEMENTDATE::date) as time,ATTRIBUTION_NETWORK_NAME,site_campaign_id,
     case when site_campaign_name is null and ATTRIBUTION_NETWORK_NAME='UNATTRIBUTED' then 'UNATTRIBUTED' else site_campaign_name end as site_campaign_name,
     count(distinct ln_client) as Total_LN0_DISBURSMENTS,
     count(distinct case when loan_product_id like '%UCLL%' then ln_client else null end ) as Personal_LN0_DISBURSMENTS,
     count(distinct case when loan_product_id like '%UCBLL%' then ln_client else null end ) as FidoBiz_LN0_DISBURSMENTS
        from (
        select distinct USER_ID_Clean,ks.user_id as kochava_user_id, date(s.CREATED_TIMESTAMP) as CREATED_TIMESTAMP ,ks.kochava_device_id, 
        ks.install_date_UTC ,s.id as fido_user_id,
        datediff(day,ks.install_date_UTC,s.CREATED_TIMESTAMP) days_diff , INSTALL_DEVICES_IP ,ATTRIBUTION_NETWORK_NAME,
        INSTALLGEO_COUNTRY_CODE3 ,ATTRIBUTION_TRACKER_NAME  ,ATTRIBUTION_CREATIVE ,ml.loan_product_id, s.type as user_type,
        ml.DISBURSEMENTDATE ,ml.LAST_EXPECTED_REPAYMENT , ml.LN, ml.LOAN_ID ,ml.DTR ,ml.IS_REPAID , site_campaign_name ,site_campaign_id, ml.client_id as ln_client
        from (Select * from ML.LOAN_INFO_TBL  where DISBURSEMENTDATE is not null and ln=0 ) ml
        left join BANKING_SERVICE.USER s on ml.client_id=s.BANKING_PLATFORM_ID
        left join installs ks on ks.USER_ID_Clean=s.id and install_date_UTC::date <= DISBURSEMENTDATE ::date
        where date(DISBURSEMENTDATE) between '{{range.start}}' and  '{{range.end}}'
        and ml.PLATFORM != 'ios'
        qualify row_number() over (partition by ml.client_id order by ks.install_date_UTC desc)=1) raw_data
        group by 1,2,3,4)
    
,SAVINGS_TO_LN0 as (
    select date_trunc('{{ time }}',sab.creationdate::date) as time,
           ATTRIBUTION_NETWORK_NAME,
           site_campaign_id,
           case when site_campaign_name is null and ATTRIBUTION_NETWORK_NAME='UNATTRIBUTED' then 'UNATTRIBUTED' else site_campaign_name end as site_campaign_name,
           count(distinct case when li.disbursementdate is not null and li.disbursementdate > sab.creationdate then sab.client_id else null end) as Savings_to_LN0
    from (
        select distinct USER_ID_Clean,
               ks.user_id as kochava_user_id, 
               date(s.CREATED_TIMESTAMP) as CREATED_TIMESTAMP,
               ks.kochava_device_id, 
               ks.install_date_UTC,
               s.id as fido_user_id,
               datediff(day,ks.install_date_UTC,s.CREATED_TIMESTAMP) days_diff,
               INSTALL_DEVICES_IP,
               ATTRIBUTION_NETWORK_NAME,
               INSTALLGEO_COUNTRY_CODE3,
               ATTRIBUTION_TRACKER_NAME,
               ATTRIBUTION_CREATIVE,
               s.type as user_type,
               sab.creationdate,
               site_campaign_name,
               site_campaign_id,
               sab.client_id,
               li.disbursementdate,
               li.ln
        from savings_account_base sab
        left join BANKING_SERVICE.USER s on sab.client_id = s.BANKING_PLATFORM_ID
        left join installs ks on ks.USER_ID_Clean = s.id and install_date_UTC::date <= sab.creationdate::date
        left join (
            select client_id, disbursementdate, ln
            from ML.LOAN_INFO_TBL 
            where disbursementdate is not null
            qualify ROW_NUMBER() OVER(partition by client_id order by disbursementdate asc) = 1
        ) li on sab.client_id = li.client_id
        where date(sab.creationdate) between '{{range.start}}' and '{{range.end}}'
        and datediff('day', date(s.CREATED_TIMESTAMP), date(sab.creationdate)) <= 30  -- Within 30 days of signup
        and date(s.CREATED_TIMESTAMP) < date(sab.creationdate)  -- Signup before savings
        qualify row_number() over (partition by sab.client_id order by ks.install_date_UTC desc) = 1
    ) raw_data
    group by 1,2,3,4
)

,installs_groups as (
select date_trunc('{{ time }}',install_date_UTC::date) as time,ATTRIBUTION_NETWORK_NAME ,
case when site_campaign_id='21416782555' then 'UAC Ghana - ROAS FS #2'
     when site_campaign_name is null and ATTRIBUTION_NETWORK_NAME='UNATTRIBUTED' then 'UNATTRIBUTED'
else site_campaign_name end as site_campaign_name,
count (distinct kochava_device_id) as installs 
from installs
where install_date_UTC::date between '{{range.start}}' and  '{{range.end}}'
group by 1,2,3)


select Event_Time, ATTRIBUTION_NETWORK_NAME, site_campaign_name,installs, signups, KYC_VERIFIED, Total_Savings_Signups, Total_LN0_DISBURSMENTS,Total_Unattributed, Unattributed_split,
Personal_LN0_DISBURSMENTS,FidoBiz_LN0_DISBURSMENTS,Savings_to_LN0,
daily_cost,
CAC,CAC_Weighted, Rate_Signups, Rate_KYC from (


select ca.time Event_Time,ca.ATTRIBUTION_NETWORK_NAME ,
ca.site_campaign_name ,installs,signups,KYC_VERIFIED,Total_Savings_Signups,Total_LN0_DISBURSMENTS,
Personal_LN0_DISBURSMENTS,FidoBiz_LN0_DISBURSMENTS,COALESCE(stl.Savings_to_LN0,0) as Savings_to_LN0, daily_cost,
case when Total_Savings_Signups >0 then daily_cost/Total_Savings_Signups else null end as Savings_CAC,
case when Total_LN0_DISBURSMENTS >0 then daily_cost/Total_LN0_DISBURSMENTS else null end as CAC,

case when ((ca.site_campaign_name is null and ca.ATTRIBUTION_NETWORK_NAME is null ) or ca.site_campaign_name='UNATTRIBUTED' or ca.ATTRIBUTION_NETWORK_NAME = 'UNATTRIBUTED') then 0 else Total_LN0_DISBURSMENTS end as attributed_,
case when ((ca.site_campaign_name is null and ca.ATTRIBUTION_NETWORK_NAME is null ) or ca.site_campaign_name='UNATTRIBUTED' or ca.ATTRIBUTION_NETWORK_NAME = 'UNATTRIBUTED') then Total_LN0_DISBURSMENTS else 0 end as Unattributed_,
sum(attributed_) over () as Total_Attributed, 
sum(Unattributed_) over () as Total_Unattributed,
Total_LN0_DISBURSMENTS/Total_Attributed as whight_attributed,
Total_Unattributed * whight_attributed as Unattributed_split,
Unattributed_split + Total_LN0_DISBURSMENTS total_ln0_new, 
case when total_ln0_new>0 then daily_cost/total_ln0_new else null end as CAC_Weighted,

case when installs is not null then signups/installs else null end as Rate_Signups,
case when signups is not null then KYC_VERIFIED/signups else null end as Rate_KYC

from installs_groups ig full join signups su on ig.time=su.time and ig.site_campaign_name=su.site_campaign_name and ig.ATTRIBUTION_NETWORK_NAME=su.ATTRIBUTION_NETWORK_NAME
                        full join SAVINGS_SIGNUPS ss on ig.time=ss.time and ss.site_campaign_name=ig.site_campaign_name and ss.ATTRIBUTION_NETWORK_NAME=ig.ATTRIBUTION_NETWORK_NAME
                        full join LN0_DISBURSMENTS ca on ig.time=ca.time and ca.site_campaign_name=ig.site_campaign_name and ca.ATTRIBUTION_NETWORK_NAME=ig.ATTRIBUTION_NETWORK_NAME
                        full join SAVINGS_TO_LN0 stl on ig.time=stl.time and stl.site_campaign_name=ig.site_campaign_name and stl.ATTRIBUTION_NETWORK_NAME=ig.ATTRIBUTION_NETWORK_NAME
                        full join cost_data cd on ig.time=cd.time and cd.partner_campaign_id=ca.site_campaign_id
--where ig.site_campaign_name not like 'UAC Uganda%'        
--where Total_LN0_DISBURSMENTS > '{{Min_ln0_DISBURSMENTS}}'
)
--where site_campaign_name in ('UAC Ghana - EasySave Signals', 'GH EasySave App Campaign')
order by Total_LN0_DISBURSMENTS desc