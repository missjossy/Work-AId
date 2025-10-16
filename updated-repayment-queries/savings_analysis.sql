WITH eligibility_scenarios AS (
  SELECT 
    client_id,
    LN,
    disbursementdate, CASE WHEN loan_product_id like '%UCBL%' then 'Fidobiz' else 'Personal' end as loan_type,
    CASE 
      WHEN LN >= 24 AND disbursementdate <= '2025-04-03' THEN DATE('2025-04-03')
      WHEN LN >= 24 AND disbursementdate > '2025-04-03' THEN DATE(disbursementdate)
      WHEN LN BETWEEN 16 AND 23 AND disbursementdate <= '2025-05-16' THEN DATE('2025-05-16')
      WHEN LN BETWEEN 16 AND 23 AND disbursementdate > '2025-05-16' THEN DATE(disbursementdate)
      ELSE NULL
    END AS eligibility_date
  FROM ml.loan_info_tbl
  WHERE LN >= 16 
    AND disbursementdate IS NOT NULL
),

all_eligible_clients AS (
  SELECT 
    client_id, loan_type,
    eligibility_date,
    MAX(LN) OVER(partition by client_id) as ln_at_eligibility 
  FROM eligibility_scenarios
  WHERE eligibility_date IN ('2025-04-03', '2025-05-16')
  --GROUP BY client_id, eligibility_date
  UNION ALL
  SELECT 
    client_id,loan_type,
    eligibility_date,
    LN as ln_at_eligibility
  FROM eligibility_scenarios
  WHERE eligibility_date NOT IN ('2025-04-03', '2025-05-16')
    AND eligibility_date IS NOT NULL
),
 login_activity AS (
    SELECT DISTINCT
        bu.banking_platform_id as client_id,
        DATE(bn.timestamp) as activity_date
    FROM data.backend_notifications bn
    JOIN banking_service.user bu ON bn.user_identity = bu.id
    WHERE bn."TYPE" = 'BE_AUTHENTICATION_PHONE_VERIFICATION_2'
    AND TO_TIMESTAMP_NTZ(bn.timestamp) between '2025-04-03' and '2025-05-31'
    
),
-- New CTE to count logins before account creation

savings_accounts as (
select sa.*, sa.id as account_id, cl.id as client_id, datediff('year', cl.BIRTHDATE, current_date) as age, st."type" as trans_type , 
st.amount as transamount, st.entrydate as trans_date, FIRST_VALUE(sa.BALANCE) OVER(partition by cl.id order by sa.creationdate desc) as last_balance,
CASE WHEN trans_type in ('WITHDRAWAL', 'DEPOSIT') then datediff('day', sa.creationdate, sa.lastmodifieddate) end as account_age,
FIRST_VALUE(account_age) OVER(partition by cl.id order by sa.creationdate desc) as last_account_age,
FIRST_VALUE(sa.creationdate) OVER(partition by cl.id order by sa.creationdate desc) as last_creation_date
FROM MAMBU.SAVINGSACCOUNT sa
LEFT JOIN GHANA_PROD.MAMBU.SAVINGSTRANSACTION st ON st.PARENTACCOUNTKEY = sa.ENCODEDKEY
LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
-- LEFT JOIN active_users au ON au.BANKING_PLATFORM_ID = c.id
LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_id ON st.ENCODEDKEY = wallet_id.PARENTKEY AND wallet_id.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'ACCOUNT_NUMBER_TRANSACTION_CHANN')
LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_network ON st.ENCODEDKEY = wallet_network.PARENTKEY AND wallet_network.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'NETWORK_TRANSACTION_CHANNEL')
LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE identifier ON st.ENCODEDKEY = identifier.PARENTKEY AND identifier.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'IDENTIFIER_TRANSACTION_CHANNEL_I')
where sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
and (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
and (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE2 is null) 
and (wallet_id.value not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or wallet_id.value is null)
-- AND "type" in ('DEPOSIT','ADJUSTMENT') 
AND date(entrydate) BETWEEN date('2025-04-03') and date('2025-05-31')
AND date(sa.creationdate) >= date('2025-04-03')
),
prior_logins_cte AS (
  SELECT
    la.client_id,
    COUNT(*) AS prior_logins
  FROM login_activity la
  JOIN (
    SELECT client_id, MIN(creationdate) AS creationdate
    FROM savings_accounts
    GROUP BY client_id
  ) sa ON la.client_id = sa.client_id
  WHERE la.activity_date < sa.creationdate
  GROUP BY la.client_id
),
-- Step 5: Get the earliest eligibility for each client
first_eligibility AS (
  SELECT 
    cs.client_id,
    client_type, loan_type,
    eligibility_date,
    ln_at_eligibility,
    CASE WHEN sa.client_id is not null then creationdate else ' ' end as signup_date,
    CASE WHEN sa.client_id is not null then 1 else 0 end as account_created,
    last_creation_date,
    a.logins,
    plc.prior_logins,
    MIN(activity_date) OVER (partition by la.client_id) as earliest_login,
    MAX(activity_date) OVER (partition by la.client_id) as last_login,
    ROW_NUMBER() OVER (PARTITION BY cs.client_id ORDER BY eligibility_date ASC) as rn
  FROM login_activity la
  LEFT JOIN all_eligible_clients cs on cs.client_id = la.client_id
  LEFT JOIN (select client_id, count(activity_date) as logins from login_activity group by 1) a on a.client_id = cs.client_id
  LEFT JOIN (
    SELECT distinct client_id, creationdate, last_creation_date 
    from savings_accounts 
    QUALIFY ROW_NUMBER() OVER(partition by client_id order by creationdate asc) = 1
  ) sa ON sa.client_id = cs.client_id
  LEFT JOIN prior_logins_cte plc ON plc.client_id = cs.client_id
  LEFT JOIN (
    select distinct client_id, loan_product_id, case when loan_product_id like '%UCBLL%' then 'FidoBiz' else 'Personal' end as client_type
    from ml.loan_info_tbl
    where disbursementdate is not null
    QUALIFY ROW_NUMBER() OVER (PARTITION BY client_id order by disbursementdate desc) = 1
  ) lp on lp.client_id = cs.client_id
  where activity_date >= eligibility_date
),

results as (
 SELECT 
  distinct fe.client_id,
  case when age <= 20 then '20 and Below'
when age between 21 and 30 then '21-30'
when age between 31 and 40 then '31-40'
when age between 41 and 50 then '41-50'
else '50+' end as age_group, 
COUNT(distinct account_id) OVER(partition by sa.client_id) as no_accounts,
--loan_type,
creationdate,
MAX(CASE WHEN trans_type in ('WITHDRAWAL', 'DEPOSIT') then trans_date end ) OVER(partition by sa.client_id) as last_trans,
last_account_age,
COUNT(CASE WHEN trans_type = 'DEPOSIT' then 1 END ) OVER(partition by sa.client_id) as deposits,
AVG(CASE WHEN trans_type = 'DEPOSIT' then transamount else null end) OVER(partition by sa.client_id) as avg_deposit_amt,
MAX(CASE WHEN trans_type = 'DEPOSIT' then transamount else null end) OVER(partition by sa.client_id) as max_deposit_amt,
COUNT(CASE WHEN trans_type = 'WITHDRAWAL' then 1 END ) OVER(partition by sa.client_id) as withdrawals,
AVG(CASE WHEN trans_type = 'WITHDRAWAL' then -transamount else null end) OVER(partition by sa.client_id) as avg_withdrawal_amt,
MAX(CASE WHEN trans_type = 'WITHDRAWAL' then -transamount else null end) OVER(partition by sa.client_id) as max_withdrawal_amt,
CASE WHEN last_balance < 1 then 'closed' WHEN last_balance between 1 and 50 then 'likely_closed' when last_balance is null then null
ELSE 'active' end as churn_status,
last_balance,
client_type,
  ln_at_eligibility AS LN,
  CASE WHEN ln_at_eligibility between 16 and 19 then 'LN16-19'
  WHEN ln_at_eligibility between 20 and 24 then 'LN20-23'
  ELSE 'LN24+' end as ln_group,
  eligibility_date AS first_eligible,
  CASE WHEN eligibility_date between earliest_login and last_login then 1 else 0 end as exposed, 
  -- CASE WHEN balance
  logins,
  prior_logins,
  -- SUM(no_logins) OVER(partition by fe.client_id) as no_logins,
  -- SUM(prior_logins) OVER(partition by fe.client_id) as prior_logins,
  account_created,
  CASE 
    WHEN LN >=24 THEN 'cohort1' 
    ELSE 'cohort2' 
  END AS cohort
FROM first_eligibility fe
left join savings_accounts sa on sa.client_id = fe.client_id
WHERE rn = 1
and 
first_eligible <= '2025-05-31'
ORDER BY fe.client_id
)
select  *
from results
where account_created = 1