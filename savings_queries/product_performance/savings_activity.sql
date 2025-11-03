WITH eligibility_scenarios AS (
  SELECT 
    client_id,
    LN,
    disbursementdate, 
    CASE WHEN loan_product_id like '%UCBL%' then 'Fidobiz' else 'Personal' end as loan_type,
    CASE 
      -- Cohort 1: LN >= 24, eligible from 2025-04-03
      WHEN LN >= 24 AND disbursementdate <= '2025-04-03' THEN DATE('2025-04-03')
      WHEN LN >= 24 AND disbursementdate > '2025-04-03' THEN DATE(disbursementdate)
      -- Cohort 2: LN 16-23, eligible from 2025-05-16
      WHEN LN BETWEEN 16 AND 23 AND disbursementdate <= '2025-05-16' THEN DATE('2025-05-16')
      WHEN LN BETWEEN 16 AND 23 AND disbursementdate > '2025-05-16' THEN DATE(disbursementdate)
      -- Cohort 3: LN 5-15, eligible from 2025-08-25
      WHEN LN BETWEEN 5 AND 15 AND disbursementdate <= '2025-08-25' THEN DATE('2025-08-25')
      WHEN LN BETWEEN 5 AND 15 AND disbursementdate > '2025-08-25' THEN DATE(disbursementdate)
      -- Cohort 4: LN 1-4, eligible from 2025-09-02
      WHEN LN BETWEEN 0 AND 4 AND disbursementdate <= '2025-09-02' THEN DATE('2025-09-02')
      WHEN LN BETWEEN 0 AND 4 AND disbursementdate > '2025-09-02' THEN DATE(disbursementdate)
      ELSE NULL
    END AS eligibility_date
  FROM ml.loan_info_tbl
  WHERE LN >= 1
  AND ACCOUNTSTATE != 'CLOSED_WRITTEN_OFF'
    AND disbursementdate IS NOT NULL
),

-- Add users who never took loans (eligible from 2025-09-02)
all_users_eligible AS (
  SELECT DISTINCT
    cl.id as client_id,
    null as LN,
    NULL as disbursementdate,
    'No Loan' as loan_type,
    DATE('2025-09-02') as eligibility_date
  FROM MAMBU.CLIENT cl
  LEFT JOIN ml.loan_info_tbl li ON cl.id = li.client_id
  WHERE li.client_id IS NULL  -- Users who never took loans
    --AND cl.creationdate <= '2025-09-02'  -- Only include users who existed before the eligibility date
),

all_eligible_clients AS (
  -- Existing loan clients with cohort eligibility
  SELECT 
    client_id, 
    loan_type,
    eligibility_date,
    MAX(LN) OVER(partition by client_id) as ln_at_eligibility 
  FROM eligibility_scenarios
  WHERE eligibility_date IN ('2025-04-03', '2025-05-16', '2025-08-25', '2025-09-02')
  
  UNION ALL
  
  SELECT 
    client_id,
    loan_type,
    eligibility_date,
    LN as ln_at_eligibility
  FROM eligibility_scenarios
  WHERE eligibility_date NOT IN ('2025-04-03', '2025-05-16', '2025-08-25', '2025-09-02')
    AND eligibility_date IS NOT NULL
    
  UNION ALL
  
  -- Add users who never took loans (Cohort 5)
  SELECT 
    client_id,
    loan_type,
    eligibility_date,
    LN as ln_at_eligibility
  FROM all_users_eligible
),

login_activity AS (
    SELECT DISTINCT
        bu.banking_platform_id as client_id,
        DATE(bn.timestamp) as activity_date
    FROM data.backend_notifications bn
    JOIN banking_service.user bu ON bn.user_identity = bu.id
    WHERE bn."TYPE" = 'BE_AUTHENTICATION_PHONE_VERIFICATION_2'
    AND TO_TIMESTAMP_NTZ(bn.timestamp) between '2025-04-03' and '2025-09-08'
),

savings_accounts as (
select sa.*, sa.id as account_id, cl.id as client_id, datediff('year', cl.BIRTHDATE, current_date) as age, st."type" as trans_type , 
st.amount as transamount, st.entrydate as trans_date, FIRST_VALUE(sa.BALANCE) OVER(partition by cl.id order by sa.creationdate desc) as last_balance,
CASE WHEN trans_type in ('WITHDRAWAL', 'DEPOSIT') then datediff('day', sa.creationdate, sa.lastmodifieddate) end as account_age,
FIRST_VALUE(account_age) OVER(partition by cl.id order by sa.creationdate desc) as last_account_age,
FIRST_VALUE(sa.creationdate) OVER(partition by cl.id order by sa.creationdate desc) as last_creation_date,
--FIRST_VALUE(sa.accountstate) OVER(partition by cl.id order by sa.creationdate desc) as last_creation_date,
FIRST_VALUE(sa.creationdate) OVER(partition by cl.id order by sa.creationdate asc) as first_creation_date
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
AND date(entrydate) BETWEEN date('2025-04-03') and date('2025-09-08')
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
  LEFT JOIN (select distinct client_id, eligibility_date from all_eligible_clients) c on c.client_id = la.client_id
  WHERE la.activity_date <= sa.creationdate and activity_date >= eligibility_date
  GROUP BY la.client_id
),
ln_at_signup_calc AS (
  SELECT 
    li.client_id,
    sa.creationdate as signup_date,
    -- Get the maximum LN that was disbursed on or before the signup date
    MAX(CASE 
      WHEN li.disbursementdate <= sa.creationdate THEN li.LN 
      ELSE 0 
    END) as ln_at_signup
  FROM (
    SELECT DISTINCT client_id, creationdate
    FROM savings_accounts 
    WHERE creationdate IS NOT NULL
  ) sa
  LEFT JOIN ml.loan_info_tbl li ON sa.client_id = li.client_id
    AND li.disbursementdate IS NOT NULL
    AND li.ACCOUNTSTATE != 'CLOSED_WRITTEN_OFF'
  GROUP BY li.client_id, sa.creationdate
),

first_eligibility AS (
  SELECT 
    cs.client_id,
    client_type, 
    loan_type,
    eligibility_date,
    cs.ln_at_eligibility,  -- LN at eligibility date
    CASE WHEN sa.client_id is not null then creationdate else ' ' end as signup_date,
    CASE WHEN sa.client_id is not null then 1 else 0 end as account_created,
    COALESCE(las.ln_at_signup, 0) as ln_at_signup,  -- LN at signup date
    last_creation_date,
    a.logins,
    plc.prior_logins,
    MIN(activity_date) OVER (partition by la.client_id) as earliest_login,
    MAX(activity_date) OVER (partition by la.client_id) as last_login,
    ROW_NUMBER() OVER (PARTITION BY cs.client_id ORDER BY eligibility_date ASC) as rn
  FROM  all_eligible_clients cs
  LEFT JOIN login_activity la on cs.client_id = la.client_id
  LEFT JOIN (select client_id, count(activity_date) as logins from login_activity group by 1) a on a.client_id = cs.client_id
  LEFT JOIN (
    SELECT distinct client_id, creationdate, last_creation_date , first_creation_date
    from savings_accounts 
    QUALIFY ROW_NUMBER() OVER(partition by client_id order by creationdate asc) = 1
  ) sa ON sa.client_id = cs.client_id
  LEFT JOIN ln_at_signup_calc las ON las.client_id = cs.client_id 
    AND las.signup_date = sa.creationdate
  LEFT JOIN prior_logins_cte plc ON plc.client_id = cs.client_id
  LEFT JOIN (
    select distinct client_id, loan_product_id, case when loan_product_id like '%UCBLL%' then 'FidoBiz' else 'Personal' end as client_type
    from ml.loan_info_tbl
    where disbursementdate is not null
    QUALIFY ROW_NUMBER() OVER (PARTITION BY client_id order by disbursementdate desc) = 1
  ) lp on lp.client_id = cs.client_id
),

results as (
 SELECT 
  distinct fe.client_id , fs.score, 
  case when age <= 20 then '20 and Below'
when age between 21 and 30 then '21-30'
when age between 31 and 40 then '31-40'
when age between 41 and 50 then '41-50'
else '50+' end as age_group, 
COUNT(distinct account_id) OVER(partition by sa.client_id) as no_accounts,
creationdate,
MAX(CASE WHEN trans_type in ('WITHDRAWAL', 'DEPOSIT') then trans_date end ) OVER(partition by sa.client_id) as last_trans,
last_account_age,
COUNT(CASE WHEN trans_type = 'DEPOSIT' then 1 END ) OVER(partition by sa.client_id) as deposits,
AVG(CASE WHEN trans_type = 'DEPOSIT' then transamount else null end) OVER(partition by sa.client_id) as avg_deposit_amt,
MAX(CASE WHEN trans_type = 'DEPOSIT' then transamount else null end) OVER(partition by sa.client_id) as max_deposit_amt,
SUM(CASE WHEN trans_type = 'DEPOSIT' then transamount else null end) OVER(partition by sa.client_id) as total_deposits,
COUNT(CASE WHEN trans_type = 'WITHDRAWAL' then 1 END ) OVER(partition by sa.client_id) as withdrawals,
AVG(CASE WHEN trans_type = 'WITHDRAWAL' then -transamount else null end) OVER(partition by sa.client_id) as avg_withdrawal_amt,
MAX(CASE WHEN trans_type = 'WITHDRAWAL' then -transamount else null end) OVER(partition by sa.client_id) as max_withdrawal_amt,
SUM(CASE WHEN trans_type = 'WITHDRAWAL' then -transamount else null end) OVER(partition by sa.client_id) as total_withdrawals,
CASE WHEN last_balance < 1 then 'closed' WHEN last_balance between 1 and 50 then 'likely_closed' when last_balance is null then null
ELSE 'active' end as churn_status,
last_balance,
client_type,
  ln_at_eligibility ,ln_at_signup,
  CASE WHEN ln_at_eligibility is null then 'No Loan'
  WHEN ln_at_eligibility between 0 and 4 then 'LN0-4'
  WHEN ln_at_eligibility between 5 and 15 then 'LN5-15'
  WHEN ln_at_eligibility between 16 and 19 then 'LN16-19'
  WHEN ln_at_eligibility between 20 and 23 then 'LN20-23'
  ELSE 'LN24+' end as ln_group,
  eligibility_date AS first_eligible,
  CASE WHEN eligibility_date between earliest_login and last_login then 1 else 0 end as exposed, 
  logins,
  prior_logins,
  account_created --,
  -- CASE 
  --   WHEN LN >= 24 THEN 'cohort1' 
  --   WHEN LN between 16 and 23 then 'cohort2'
  --   WHEN LN between 5 and 15 then 'cohort3'
  --   -- WHEN LN between 1 and 4 then 'cohort4'
  --   -- WHEN LN = 0 then 'cohort5'
  --   ELSE 'cohort4' 
  -- END AS cohort
FROM first_eligibility fe
left join savings_accounts sa on sa.client_id = fe.client_id
left join ( 
select fs.client_id, fs.score
from savings_accounts sa
left join data.fido_score fs on sa.client_id = fs.client_id
where fs.created_on < sa.first_creation_date
QUALIFY ROW_NUMBER() OVER(partition by fs.client_id order by created_on desc) = 1
) fs on fs.client_id = fe.client_id --and date(eligibility_date) = date(fs.created_on)
WHERE rn = 1
ORDER BY fe.client_id
)
select *
-- date_trunc('month', creationdate) as period, sum(account_created) over(ORDER BY period 
--         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
from results
where account_created = 1
--group by 1
-- select 
-- deposit_frequency,
-- SUM(account_created) as no_users, 
-- avg(last_account_age) as avg_age, 
-- avg(withdrawals), 
-- avg(deposits), 
-- avg(avg_deposit_amt),
-- sum(total_deposits), 
-- sum(total_withdrawals), 
-- sum(total_deposits) - sum(total_withdrawals) as balance
-- from 
-- (select *, 
-- MEDIAN(deposits) OVER() dep_median, 
-- MEDIAN(withdrawals) OVER() with_median,
-- CASE WHEN deposits < 2 then 'one-time' else 'multiple' end as deposit_frequency,
-- CASE WHEN withdrawals < 5 then 'low' else 'high' end as withdrawal_frequency
-- from results
-- where account_created = 1)
-- group by 1


