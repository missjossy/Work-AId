
WITH total_unique_attempts AS (
	select date_, count(*) as total_attempts
	from (
		SELECT DISTINCT to_char(date_trunc('{{Scale}}',CREATIONDATE),'YYYY-MM-DD') date_, ml.CLIENT_KEY
		from GHANA_PROD.ML.LOAN_INFO_TBL ml
		WHERE date(CREATIONDATE) between '{{ Range.start }}' and '{{ Range.end }}'
		ORDER BY date_
	)
	group by date_
	order by date_
),
total_unique_attempts_ln0 AS (
	select date_, count(*) as total_attempts
	from (
		SELECT DISTINCT to_char(date_trunc('{{Scale}}',CREATIONDATE),'YYYY-MM-DD') date_, ml.CLIENT_KEY
		from GHANA_PROD.ML.LOAN_INFO_TBL ml
		WHERE date(CREATIONDATE) between '{{ Range.start }}' and '{{ Range.end }}'
		AND LN = 0
		ORDER BY date_
	)
	group by date_
	order by date_
),
total_survey_filled AS (
    SELECT date_, count(*) as survey_filled
    from (
    	    select client_id, to_char(date_trunc('{{Scale}}',date(LOAN_DATE)),'YYYY-MM-DD') date_
    	    from GHANA_PROD.DATA.SURVEY_DATA 
    	    QUALIFY row_number() over (partition by client_id order by LOAN_DATE DESC) = 1
         ) 
    WHERE date_ between '{{ Range.start }}' and '{{ Range.end }}'
    group by date_
    order by date_
),
total_unique_disbursements AS (
	select date_, count(*) as total_disbursements 
	from (
		SELECT DISTINCT to_char(date_trunc('{{Scale}}',DISBURSEMENTDATE),'YYYY-MM-DD') date_, ml.CLIENT_KEY
		from GHANA_PROD.ML.LOAN_INFO_TBL ml
		WHERE date(DISBURSEMENTDATE) between '{{ Range.start }}' and '{{ Range.end }}'
		ORDER BY date_
	)
	group by date_
	order by date_
),
total_unique_disbursements_ln0 AS (
	select date_, count(case when LOAN_PRODUCT_ID like '%UCLL%' then CLIENT_KEY else null end) as Personal_disbursements_ln0,
	count(case when LOAN_PRODUCT_ID like '%UCBLL%' then CLIENT_KEY else null end) as FidoBiz_disbursements_ln0,
	count(case when (LOAN_PRODUCT_ID like '%TRSP%' or LOAN_PRODUCT_ID like '%SCTF%') then CLIENT_KEY else null end) as First_Disb_Partnerships,
	count (CLIENT_KEY) as total_disbursements_ln0
	from (
		SELECT DISTINCT to_char(date_trunc('{{Scale}}',DISBURSEMENTDATE),'YYYY-MM-DD') date_,LOAN_PRODUCT_ID, ml.CLIENT_KEY
		from GHANA_PROD.ML.LOAN_INFO_TBL ml
		WHERE date(DISBURSEMENTDATE) between '{{ Range.start }}' and '{{ Range.end }}'
		AND LN = 0 
		--and ml.LOAN_PRODUCT_ID LIKE '%UCLL%'
		ORDER BY date_
	)
	group by date_
	order by date_
),
kyc_verified AS (  
select date_, count(*) as KYC_VERIFIED
    from (
    SELECT DISTINCT to_char(date_trunc('{{Scale}}',date(TIMESTAMP)),'YYYY-MM-DD') as date_, USER_IDENTITY
    		FROM GHANA_PROD.DATA.BACKEND_NOTIFICATIONS WHERE TYPE='BE_KYC_VERIFICATION_RESULT'
            AND parse_json(payload):status = 'succeeded'
            AND parse_json(payload):is_duplicate = false
            AND date(TIMESTAMP) BETWEEN '{{ Range.start }}' and '{{ Range.end }}'
    	)
    group by date_
    order by date_
),
Duplicat_Users_Ratio as (
    SELECT  to_char(date_trunc('{{Scale}}',date(TIMESTAMP)),'YYYY-MM-DD') as date_,
            count(DISTINCT(case when parse_json(payload):is_duplicate = true then USER_IDENTITY end )) duplicate_users,
            count(DISTINCT USER_IDENTITY) all_kyc,
            duplicate_users/all_kyc Duplicat_Users_Ratio
    		FROM GHANA_PROD.DATA.BACKEND_NOTIFICATIONS WHERE TYPE='BE_KYC_VERIFICATION_RESULT'
            AND parse_json(payload):status = 'succeeded'
            AND date(TIMESTAMP) BETWEEN '{{ Range.start }}' and '{{ Range.end }}'
    group by date_
    order by date_
),
sign_ups as (
    select date_, count(*) as sign_ups
    from (
    		SELECT DISTINCT to_char(date_trunc('{{Scale}}',CREATED_TIMESTAMP),'YYYY-MM-DD') date_, USER.ID
    		FROM GHANA_PROD.BANKING_SERVICE."USER" 
    		WHERE date(CREATED_TIMESTAMP) BETWEEN '{{ Range.start }}' and '{{ Range.end }}'
    	)
    group by date_
    order by date_
),
first_fs_above_250_fido_score_table as (
    select to_char(date_trunc('{{Scale}}',created_on),'YYYY-MM-DD') date_, count(distinct client_id) as fs_eligible_fs_table 
    from (
          select created_on ,score,client_id
          from GHANA_PROD.data.fido_score c 
          QUALIFY ROW_NUMBER () over (PARTITION BY CLIENT_ID ORDER BY created_on asc)=1 
         )
    where  date(created_on) between '{{ Range.start }}' and '{{ Range.end }}'  and score >= 250      
    group by date_
    order by date_
),
first_fs_above_250 as (
    select date_, count(*) as fs_eligible
    from (
          select DISTINCT client_id ,to_char(date_trunc('{{Scale}}',survey_date),'YYYY-MM-DD') date_ 
          from GHANA_PROD.ML.client_info c 
          where date(survey_date) between '{{ Range.start }}' and '{{ Range.end }}'
          and first_fido_score >= 250 and (FRAUD_TYPE_MATCHED ='' or FRAUD_TYPE_MATCHED is null)
          and (PERSONAL_BR_DECISION is null or PERSONAL_BR_DECISION='APPROVED'))
    group by date_
    order by date_
),
Eligable_Blocked as (
    select date_, count(*) as Eligable_Blocked
    from (
          select DISTINCT client_id ,to_char(date_trunc('{{Scale}}',survey_date),'YYYY-MM-DD') date_ 
          from GHANA_PROD.ML.client_info c 
          where date(survey_date) between '{{ Range.start }}' and '{{ Range.end }}'
          and first_fido_score >= 250 and FRAUD_TYPE_MATCHED != '' 
          and PERSONAL_BR_DECISION is not null )
    group by date_
    order by date_
),
savings_data as (
select sa.*, sa.id as account_id, cl.id as client_id, datediff('year', cl.BIRTHDATE, '{{ Range.end }}') as age, st."type" as trans_type , sa.creationdate as acct_created_date, 
    st.amount as transamount, st.entrydate as trans_date, -- sa.creationdate, 
    MAX(CASE WHEN trans_type = 'DEPOSIT' then st.entrydate end) OVER(partition by cl.id) as last_deposit,
    FIRST_VALUE(sa.BALANCE) OVER(partition by cl.id order by sa.creationdate desc) as last_balance,
    CASE WHEN trans_type in ('WITHDRAWAL', 'DEPOSIT') then datediff('day', sa.creationdate, sa.lastmodifieddate) end as account_age,
    FIRST_VALUE(account_age) OVER(partition by cl.id order by sa.creationdate desc) as last_account_age,
    FIRST_VALUE(sa.creationdate) OVER(partition by cl.id order by sa.creationdate desc) as last_creation_date,
    -- Add period grouping for monthly analysis
    date_trunc('month', st.entrydate) as transaction_month
FROM MAMBU.SAVINGSACCOUNT sa
LEFT JOIN GHANA_PROD.MAMBU.SAVINGSTRANSACTION st ON st.PARENTACCOUNTKEY = sa.ENCODEDKEY
LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_id ON st.ENCODEDKEY = wallet_id.PARENTKEY AND wallet_id.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'ACCOUNT_NUMBER_TRANSACTION_CHANN')
LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_network ON st.ENCODEDKEY = wallet_network.PARENTKEY AND wallet_network.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'NETWORK_TRANSACTION_CHANNEL')
LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE identifier ON st.ENCODEDKEY = identifier.PARENTKEY AND identifier.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'IDENTIFIER_TRANSACTION_CHANNEL_I')
where sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
and (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
and (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE2 is null) 
and (wallet_id.value not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or wallet_id.value is null)
AND date(entrydate) BETWEEN date('2025-04-03') and date('{{ Range.end }}')
AND sa.accountstate !='WITHDARWN'
AND date(sa.creationdate) >= date('2025-04-03')), 

all_savings_accounts AS ( 
SELECT 
date_trunc('{{Scale}}', sa.creationdate) date_ , 
count(distinct sa.accountholderkey) as new_daily_accounts, 
count(distinct case when sa.accountstate = 'ACTIVE' then sa.accountholderkey END) As new_daily_active_accounts
FROM mambu.savingsaccount sa
LEFT JOIN mambu.client cl ON cl.encodedkey = sa.accountholderkey
LEFT JOIN mambu.customfieldvalue wallet_id ON sa.encodedkey = wallet_id.parentkey 
    AND wallet_id.customfieldkey = (SELECT encodedkey FROM mambu.customfield WHERE id = 'ACCOUNT_NUMBER_TRANSACTION_CHANN')
WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
    AND sa.accountstate !='WITHDRAWN'
    AND sa.creationdate IS NOT NULL
    AND DATE(sa.creationdate) >= date('2025-04-03')
    AND (cl.MOBILEPHONE1 NOT IN ('233552602681', '233243630046', '233245822584', '23346722591', '233257806345') OR cl.MOBILEPHONE1 IS NULL)
    AND (cl.MOBILEPHONE2 NOT IN ('233552602681', '233243630046', '233245822584', '23346722591', '233257806345') OR cl.MOBILEPHONE2 IS NULL) 
    AND (wallet_id.value NOT IN ('233552602681', '233243630046', '233245822584', '23346722591', '233257806345') OR wallet_id.value IS NULL)
AND date(sa.creationdate) BETWEEN date('2025-04-03') and date('{{ Range.end }}')
group by date_ 
order by date_), 

active_savings_account as (
select date_ , 
SUM(new_daily_active_accounts) OVER(ORDER BY date_ ROWS UNBOUNDED PRECEDING) total_active_accounts 
from all_savings_accounts ),

period as (SELECT DATE('{{Range.start}}') AS date_
    
    UNION ALL
    
    SELECT DATEADD(DAY, 1, date_)
    FROM period
    WHERE date_ < DATE('{{Range.end}}'))
, 

active_users_by_period as (
    select p.date_, 
    count(distinct sd.account_id)
    from period p 
    join savings_data sd on 
    date(sd.acct_created_date) <= p.date_ 
    and ((sd.ACCOUNTSTATE = 'ACTIVE')
         OR (sd.ACCOUNTSTATE = 'CLOSED' and date(sd.closeddate) > p.date_))
    group by 1
    
)


, 

fs_finished as (
    select date_, count(*) as fs_finished
    from (
          select DISTINCT client_id ,to_char(date_trunc('day',survey_date),'YYYY-MM-DD') date_
          from GHANA_PROD.ML.client_info c 
          where date(survey_date) between '{{ Range.start }}' and '{{ Range.end }}'
        )
    group by date_
    order by date_
)


,fidobiz as (
SELECT to_char(date_trunc('{{Scale}}',DISBURSEMENTDATE),'YYYY-MM-DD') date_ ,
count (distinct case when ln =0  and LOAN_PRODUCT_ID like '%UCBLL%' then CLIENT_ID else null end ) Fidobiz_New,
count (distinct case when ln between 1 and 3  and LOAN_PRODUCT_ID like '%UCBLL%' then CLIENT_ID else null end ) Fidobiz_Migrated_New,
count (distinct case when ln>3 and LOAN_PRODUCT_ID like '%UCBLL%' then CLIENT_ID else null end ) Fidobiz_Migrated_old ,
count (distinct case when ln =0  and (LOAN_PRODUCT_ID like '%TRSP%' or LOAN_PRODUCT_ID like '%SCTF%' or LOAN_PRODUCT_ID like '%UCBPLL%') then CLIENT_ID else null end ) Partherships_New,
count (distinct case when ln between 1 and 3  and (LOAN_PRODUCT_ID like '%TRSP%' or LOAN_PRODUCT_ID like '%SCTF%' or LOAN_PRODUCT_ID like '%UCBPLL%' )  then CLIENT_ID else null end ) Partherships_Migrated_New,
count (distinct case when ln>3 and (LOAN_PRODUCT_ID like '%TRSP%' or LOAN_PRODUCT_ID like '%SCTF%' or LOAN_PRODUCT_ID like '%UCBPLL%')  then CLIENT_ID else null end ) Partherships_Migrated_old 
FROM (
	SELECT CLIENT_KEY ,CLIENT_ID, creationdate, DISBURSEMENTDATE, LN, LOAN_PRODUCT_ID, 
	case when LOAN_PRODUCT_ID like '%MAL%' then 'Momo_Agents_POC'
	           when LOAN_PRODUCT_ID like '%UCBLL%' then 'FidoBiz'
	           when (LOAN_PRODUCT_ID like '%TRSP%' or LOAN_PRODUCT_ID like '%UCBPLL%') then 'Bolt'
	           when LOAN_PRODUCT_ID like '%SCTF%' then 'Unilever' end as product_type,
	       LAG(IFF(DISBURSEMENTDATE is not null,1,0),1,0) over (PARTITION BY CLIENT_ID ORDER BY creationdate  ASC) as prev_disbursed,
	       CONDITIONAL_TRUE_EVENT(prev_disbursed=1) OVER (PARTITION BY CLIENT_ID ORDER BY creationdate ASC) as new_ln
	FROM GHANA_PROD.ml.LOAN_INFO_TBL 
	WHERE case when LOAN_PRODUCT_ID like '%MAL%' then 'Momo_Agents_POC'
	           when LOAN_PRODUCT_ID like '%UCBLL%' then 'FidoBiz'
	           when (LOAN_PRODUCT_ID like '%TRSP%' or LOAN_PRODUCT_ID like '%UCBPLL%') then 'Bolt'
	           when LOAN_PRODUCT_ID like '%SCTF%' then 'Unilever'
	           end IN ('Momo_Agents_POC','FidoBiz','Bolt','Unilever')
)
---	WHERE case when LOAN_PRODUCT_ID like '%MAL%' then 'Momo_Agents_POC'
	  ---         when LOAN_PRODUCT_ID like '%UCBLL%' then 'FidoBiz' end IN ('FidoBiz')
where date(DISBURSEMENTDATE) between '{{ Range.start }}' and '{{ Range.end }}'
and new_ln = 0
group by 1 )   

,kyb as 
(select to_char(date_trunc('{{Scale}}',timestamp::date),'YYYY-MM-DD') date_,
count(distinct USER_IDENTITY) as KYB_submitted 
from (
select *,
case when (disbursementdate is null or ln=0 ) then 'New'
when ln between 1 and 3 then 'Migrated-New'
when ln>3 then 'Migrated-Old' end as Client_Type_1 
from (
select *,parse_json(payload):application_info:application_version::string as app_version 
from DATA.BACKEND_NOTIFICATIONS where type ='BE_FIDOBIZ_SURVEY_SUBMISSION'
qualify row_number () over (partition by USER_IDENTITY order by timestamp)=1 ) app 
LEFT JOIN BANKING_SERVICE.USER_BANK_USER s ON app.USER_IDENTITY =s.USER_ID 
LEFT JOIN (
SELECT client_id , disbursementdate, LN FROM GHANA_PROD.ml.LOAN_INFO_TBL ml WHERE ml.disbursementdate IS NOT NULL 
AND ml.LOAN_PRODUCT_ID not LIKE '%UCBLL%'
QUALIFY ROW_NUMBER () over (PARTITION BY CLIENT_ID ORDER BY DISBURSEMENTDATE desc)=1 ) ml 
ON ml.CLIENT_ID =s.BANKING_PLATFORM_ID 
where date(app.timestamp) between '{{ Range.start }}' and '{{ Range.end }}')
where Client_Type_1 IN ('New')
group by 1)
 
,biz_doc as 
(select to_char(date_trunc('{{Scale}}',timestamp::date),'YYYY-MM-DD') date_,
count(distinct USER_IDENTITY) as biz_doc 
from (
select *,
case when (disbursementdate is null or ln=0 ) then 'New'
when ln between 1 and 3 then 'Migrated-New'
when ln>3 then 'Migrated-Old' end as Client_Type_1 
from (
select *,parse_json(payload):updated_status::string as doc_status 
from DATA.BACKEND_NOTIFICATIONS where type ='BE_FIDOBIZ_SCORE_DOCUMENT_STATUS_EVENT'
and doc_status='IN_REVIEW'
qualify row_number () over (partition by USER_IDENTITY order by timestamp)=1 ) app 
LEFT JOIN BANKING_SERVICE.USER_BANK_USER s ON app.USER_IDENTITY =s.USER_ID 
LEFT JOIN (
SELECT client_id , disbursementdate, LN FROM GHANA_PROD.ml.LOAN_INFO_TBL ml WHERE ml.disbursementdate IS NOT NULL 
AND ml.LOAN_PRODUCT_ID not LIKE '%UCBLL%'
QUALIFY ROW_NUMBER () over (PARTITION BY CLIENT_ID ORDER BY DISBURSEMENTDATE desc)=1 ) ml 
ON ml.CLIENT_ID =s.BANKING_PLATFORM_ID 
where date(app.timestamp) between '{{ Range.start }}' and '{{ Range.end }}')
where Client_Type_1 IN ('New')
group by 1)


 

select total_unique_attempts.date_,
    DAYNAME(total_unique_attempts.date_) as weekday,
       total_unique_attempts.date_ || ' (' || DAYNAME(total_unique_attempts.date_) || ')' as date_with_weekday,
       sign_ups.sign_ups,
       kyc_verified.kyc_verified, 
       first_fs_above_250.fs_eligible,
       first_fs_above_250_fido_score_table.fs_eligible_fs_table , 
       KYB_submitted,
       biz_doc,
       Personal_disbursements_ln0,
       Fidobiz_New as First_Dis_Fidobiz_New,
       Partherships_New as First_Disb_Partnerships,
       Fidobiz_Migrated_New as First_Dis_Fidobiz_Migrated_New,
       Partherships_Migrated_New,
       Fidobiz_Migrated_old as First_Dis_Fidobiz_Migrated_Old,
       Partherships_Migrated_old,
       total_disbursements_ln0,
       total_unique_attempts_ln0.total_attempts AS total_attempts_ln0,
       total_survey_filled.survey_filled,
       total_unique_attempts.total_attempts, 
       total_unique_disbursements.total_disbursements,
       kyc_verified/sign_ups*100 reg_kyc_ratio,
       total_disbursements_ln0/kyc_verified*100 kyc_disb_ratio, --all_savings_accounts.new_daily_accounts as New_Daily_Saving_Accounts , 
       active_savings_account.total_active_accounts as total_active_saving, 
       all_savings_accounts.new_daily_accounts as new_saving_accounts ,
       total_disbursements_ln0/fs_eligible_fs_table as eligable_to_dis_rate,
       total_disbursements_ln0/sign_ups as signup_to_Ln0,
       fs_finished.fs_finished,
       Eligable_Blocked,
       Duplicat_Users_Ratio.duplicate_users,
       Duplicat_Users_Ratio.Duplicat_Users_Ratio,
       survey_filled/kyc_verified as survey_filled_rate, 
       first_fs_above_250_fido_score_table.fs_eligible_fs_table/kyc_verified.kyc_verified*100 kyc_to_eligible_ratio
from total_unique_attempts
full join total_unique_attempts_ln0 on total_unique_attempts.date_ = total_unique_attempts_ln0.date_ 
full join total_survey_filled on total_unique_attempts.date_ = total_survey_filled.date_ 
full join total_unique_disbursements on total_unique_disbursements.date_ = total_unique_attempts.date_ 
full join total_unique_disbursements_ln0 on total_unique_disbursements_ln0.date_ = total_unique_attempts.date_ 
full join kyc_verified on kyc_verified.date_ = total_unique_attempts.date_ 
full join Duplicat_Users_Ratio on Duplicat_Users_Ratio.date_ = total_unique_attempts.date_ 
full join sign_ups on sign_ups.date_ = total_unique_attempts.date_ 
full join first_fs_above_250 on first_fs_above_250.date_ = total_unique_attempts.date_ 
full join first_fs_above_250_fido_score_table on first_fs_above_250_fido_score_table.date_ = total_unique_attempts.date_
full join fs_finished on fs_finished.date_ = total_unique_attempts.date_ 
full join Eligable_Blocked on Eligable_Blocked.date_ = total_unique_attempts.date_ 
full join fidobiz on fidobiz.date_ = total_unique_attempts.date_ 
full join kyb on kyb.date_=total_unique_attempts.date_ 
full join biz_doc on biz_doc.date_=total_unique_attempts.date_ 
 join all_savings_accounts on all_savings_accounts.date_ = total_unique_attempts.date_
join active_savings_account on active_savings_account.date_ = total_unique_attempts.date_ 
order by total_unique_attempts.date_





