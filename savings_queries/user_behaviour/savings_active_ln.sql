with savings_details as (
SELECT cl.id as client_id, sa.id as account_id, st.PARENTACCOUNTKEY, entrydate, sa.creationdate, sa.balance, sa.lastmodifieddate,sa.closeddate, st.amount, accountstate, 
FIRST_VALUE(sa.creationdate) OVER(partition by cl.id order by sa.creationdate asc) as first_creation_date
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
AND date(sa.creationdate) between '{{Range.start}}' and '{{Range.end}}'
--and cl.id =  '304489447'
),
-- Calculate LN at savings creation (similar to savings_activity.sql)
ln_at_signup_calc AS (
  SELECT 
    sa.client_id,
    sa.first_creation_date as signup_date,
    -- Get the maximum LN that was disbursed on or before the signup date
  MAX(CASE 
      WHEN li.disbursementdate <= sa.first_creation_date THEN li.LN 
      ELSE null
    END) as ln_at_signup
  FROM savings_details sa
  LEFT JOIN (select * from ml.loan_info_tbl
  WHERE disbursementdate IS NOT NULL)
  li ON sa.client_id = li.client_id
   WHERE li.ACCOUNTSTATE != 'CLOSED_WRITTEN_OFF'
  GROUP BY sa.client_id, sa.first_creation_date
),

-- ln_at_signup_calc AS (
--   SELECT 
--     sd.client_id,
--     sd.creationdate as signup_date,
--     -- Get the maximum LN that was disbursed on or before the signup date
--     COALESCE(MAX(CASE 
--       WHEN li.disbursementdate <= sd.creationdate THEN li.LN 
--       ELSE null
--     END), null) as ln_at_signup
--   FROM savings_details sd
--   LEFT JOIN ml.loan_info_tbl li ON sd.client_id = li.client_id
--     AND li.disbursementdate IS NOT NULL
--     AND li.ACCOUNTSTATE != 'CLOSED_WRITTEN_OFF'
--   WHERE sd.creationdate IS NOT NULL
--   GROUP BY sd.client_id, sd.creationdate
-- ),

loan_details as (
    select --distinct 
        sd.client_id,sd.account_id,
        sd.creationdate,  
        sd.closeddate as last_modified_date,
        sd.accountstate, 
        --u.created_timestamp, 
        fs_at_signup.score as score,
        COALESCE(las.ln_at_signup, null) as ln_at_signup
    from savings_details sd 
    left join (
        select 
            fs.client_id, 
            fs.score, 
            fs.created_on,
            sd.first_creation_date,
            --ROW_NUMBER() OVER (PARTITION BY fs.client_id ORDER BY fs.created_on DESC) as rn
        from data.fido_score fs
        left join (select distinct client_id, first_creation_date from savings_details) sd
            on fs.client_id = sd.client_id
        where fs.created_on <= sd.first_creation_date
            AND FIDO_SCORE_FLOW != 'FIDOBIZ_SCORE'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY fs.client_id ORDER BY fs.created_on DESC)  = 1
    ) fs_at_signup on fs_at_signup.client_id = sd.client_id 
        --AND fs_at_signup.rn = 1
    left join ln_at_signup_calc las ON las.client_id = sd.client_id 
        --AND las.signup_date = sd.creationdate
),
-- Get all accounts with their details (one row per account)
account_details as (
    select 
        client_id, account_id,
        ln_at_signup as ln, 
        creationdate,
        last_modified_date,
        accountstate
    from loan_details
),

-- Get all distinct periods from account creation dates
periods as (
    select distinct date_trunc('month', creationdate) as period
    from account_details
    order by 1
),

-- For each period, count active accounts at the end of that period, grouped by LN Group
active_accounts_by_period as (
    select 
        p.period,--ad.client_id,
        case when ad.ln is null and ad.score < 250 then 'Ineligible New' 
        when ad.ln >=0 and ad.score < 250 then 'Returning Ineligible'
        when ad.ln is null and ad.score >= 250 then 'Eligible New'
        when ad.ln = 0 then 'LN0'
        when ad.ln between 1 and 5 then 'LN1-5'
        when ad.ln between 6 and 9 then 'LN6-9'
        when ad.ln between 10 and 12 then 'LN10-12'
        when ad.ln between 13 and 23 then 'LN13-23'
        else 'LN24+' end as "LN Group" ,
        count(distinct ad.account_id) as "Active Accounts"
    from periods p
    cross join (select ad.client_id , ad.ln, ad.account_id, ld.score, ad.creationdate,ad.last_modified_date,ad.accountstate  
    from  account_details ad
    left join loan_details ld on ad.client_id = ld.client_id) ad
    where 
        -- Account must be created before or at the end of this period
        date(ad.creationdate) <= last_day(p.period)
        -- Apply active logic as of the end of this period
        and (
            case 
                when ad.accountstate = 'ACTIVE' then 'active'
                when (ad.accountstate = 'CLOSED'  and date(ad.last_modified_date) > last_day(p.period)) then 'active'
                --when (ad.accountstate = 'CLOSED' ) then 'active'
                else 'closed' 
            end
        ) = 'active'
    group by p.period, 
        case when ad.ln is null and ad.score < 250 then 'Ineligible New' 
        when ad.ln >=0 and ad.score < 250 then 'Returning Ineligible'
        when ad.ln is null and ad.score >= 250 then 'Eligible New'
        when ad.ln = 0 then 'LN0'
        when ad.ln between 1 and 5 then 'LN1-5'
        when ad.ln between 6 and 9 then 'LN6-9'
        when ad.ln between 10 and 12 then 'LN10-12'
        when ad.ln between 13 and 23 then 'LN13-23'
        else 'LN24+' end
)

-- Add loan details for potential segmentation (keeping your original logic)
-- select * from active_accounts_by_period
-- where period = '2025-09-01'
--and "LN Group" = 'Returning Ineligible'

select 
    period, 
    "LN Group",
    "Active Accounts" as "No of Active Accounts"
from active_accounts_by_period
order by period , "LN Group" desc;