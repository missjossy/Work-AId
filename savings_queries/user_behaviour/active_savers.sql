with transaction_data as 
(
select sa.*, sa.id as account_id, cl.id as client_id, datediff('year', cl.BIRTHDATE, '{{Up To Date}}') as age, st."type" as trans_type , 
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
AND date(entrydate) BETWEEN date('2025-04-03') and date('2025-09-16')
AND date(sa.creationdate) >= date('2025-04-03')
),

-- Calculate monthly activity for each client (aggregating across all their accounts)
monthly_activity as (
select 
    client_id,
    transaction_month,
    count(case when trans_type = 'DEPOSIT' then 1 end) as monthly_deposits,
    count(case when trans_type = 'WITHDRAWAL' then 1 end) as monthly_withdrawals,
    sum(case when trans_type = 'DEPOSIT' then transamount else 0 end) as monthly_deposit_amount,
    sum(case when trans_type = 'WITHDRAWAL' then transamount else 0 end) as monthly_withdrawal_amount,
    sum(case when trans_type = 'DEPOSIT' then transamount else 0 end) + sum(case when trans_type = 'WITHDRAWAL' then transamount else 0 end) as monthly_net_amount,
    count(distinct account_id) as accounts_with_activity
from transaction_data
group by client_id, transaction_month
),

-- Identify active months for each client
active_months as (
select 
    client_id,
    transaction_month,
    case when 
        monthly_deposits >= 1  -- At least one deposit in the month
        and monthly_net_amount > 0  -- Positive net balance (deposits > withdrawals)
    then 1 else 0 end as is_active_month
from monthly_activity
),

-- Calculate client-level metrics (aggregating across all their accounts)
client_base as 
(
select 
    client_id,
    min(creationdate) as first_account_creation,  -- First account creation date
    max(creationdate) as last_account_creation,   -- Most recent account creation date
    max(last_deposit) as last_deposit_across_accounts,  -- Last deposit across all accounts
    count(distinct account_id) as total_accounts,
    count(*) as total_transactions,  -- All transactions across all accounts
    sum(case when trans_type = 'DEPOSIT' then 1 else 0 end) as no_deposits,  -- All deposits across all accounts
    sum(case when trans_type = 'WITHDRAWAL' then 1 else 0 end) as no_withdrawals,  -- All withdrawals across all accounts
    sum(transamount) as total_balance,  -- All transaction amounts across all accounts
    sum(case when trans_type = 'DEPOSIT' then transamount else 0 end) as total_deposits,  -- All deposit amounts across all accounts
    sum(case when trans_type = 'WITHDRAWAL' then transamount else 0 end) as total_withdrawals,  -- All withdrawal amounts across all accounts
    -- Count of active months for this client across all accounts
    (select count(*) from active_months am where am.client_id = td.client_id and am.is_active_month = 1) as active_months_count,
    -- Total months with any activity for this client across all accounts
    (select count(distinct transaction_month) from monthly_activity ma where ma.client_id = td.client_id) as total_active_months
from transaction_data td
group by 1
),

-- Get all distinct periods from account creation dates
periods as (
    select distinct date_trunc('month', creationdate) as period
    from transaction_data
    where creationdate is not null
    order by 1
),

-- Calculate active users by month (using savings_active_ln logic)
active_users_by_period as (
    select 
        p.period,
        count(distinct td.client_id) as active_users_count
    from periods p
    cross join (
        select distinct 
            client_id, 
            account_id,
            creationdate,
            lastmodifieddate,
            accountstate
        from transaction_data
    ) td
    where 
        -- Account must be created before or at the end of this period
        date(td.creationdate) <= last_day(p.period)
        -- Apply active logic as of the end of this period
        and (
            case 
                when td.accountstate = 'ACTIVE' then 'active'
                when (td.accountstate = 'CLOSED' and date(td.lastmodifieddate) > last_day(p.period)) then 'active'
                else 'closed' 
            end
        ) = 'active'
    group by p.period
)

-- Monthly active saver analysis by client (aggregating across all their accounts)
select 
    mas.transaction_month,
    aup.active_users_count as total_active_users_in_month,
    count(distinct mas.client_id) as clients_with_transactions_in_month,
    sum(case when mas.is_active_in_month = 1 then 1 else 0 end) as active_saver_clients_in_month,
    (sum(case when mas.is_active_in_month = 1 then 1 else 0 end) / aup.active_users_count) * 100 as monthly_active_saver_rate
from
(select 
    ma.client_id,
    ma.transaction_month,
    ma.monthly_deposits,
    ma.monthly_net_amount,
    ma.accounts_with_activity,
    -- Check if client is active in this specific month (across all their accounts)
    case when 
        ma.monthly_deposits >= 1  -- At least one deposit in the month
        and ma.monthly_net_amount > 0  -- Positive net balance (deposits > withdrawals)
    then 1 else 0 end as is_active_in_month
from monthly_activity ma
-- Only include clients who have been active for more than 1 month total across all accounts
inner join client_base cb on cb.client_id = ma.client_id
where cb.total_active_months > 1
) mas
inner join active_users_by_period aup on aup.period = mas.transaction_month
group by mas.transaction_month, aup.active_users_count
order by mas.transaction_month