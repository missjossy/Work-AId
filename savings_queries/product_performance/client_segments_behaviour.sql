WITH base_savings_data AS (
    SELECT DISTINCT
        cl.id as client_id, 
        sa.id as account_id,
        sa.creationdate as creation_date,
        sa.closeddate,
        sa.accountstate,
        sa.encodedkey as account_key,
        cl.birthdate,
        DATEDIFF('year', cl.birthdate, current_date) as age
    FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa
    LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
    WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
    AND (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
    AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE2 is null) 
    AND date(sa.creationdate) >= date('2025-04-03')
    AND date(sa.creationdate) between '2025-04-03' and '2025-10-15'
),

transaction_data AS (
    SELECT 
        st.parentaccountkey as account_key,
        st.creationdate as transaction_date,
        st.amount,
        st."type" as transaction_type,
        st.balance
    FROM GHANA_PROD.MAMBU.SAVINGSTRANSACTION st
    WHERE st."type" IN ('WITHDRAWAL', 'DEPOSIT')
    AND date(st.creationdate) BETWEEN date('2025-04-03') and date('2025-10-15')
),

client_summary AS (
    SELECT 
        bsd.client_id,
        bsd.age,
        CASE WHEN bsd.age <= 20 THEN '20 and Below'
             WHEN bsd.age between 21 and 30 THEN '21-30'
             WHEN bsd.age between 31 and 40 THEN '31-40'
             WHEN bsd.age between 41 and 50 THEN '41-50'
             ELSE '50+' END as age_group,
        COUNT(DISTINCT bsd.account_id) as total_accounts,
        MIN(bsd.creation_date) as first_account_creation,
        MAX(bsd.creation_date) as last_account_creation,
        MAX(bsd.closeddate) as last_account_closure,
        -- Transaction metrics across all accounts
        COUNT(CASE WHEN td.transaction_type = 'DEPOSIT' THEN 1 END) as total_deposits_count,
        COUNT(CASE WHEN td.transaction_type = 'WITHDRAWAL' THEN 1 END) as total_withdrawals_count,
        SUM(CASE WHEN td.transaction_type = 'DEPOSIT' THEN td.amount ELSE 0 END) as total_deposits_amount,
        SUM(CASE WHEN td.transaction_type = 'WITHDRAWAL' THEN td.amount ELSE 0 END) as total_withdrawals_amount,
        AVG(CASE WHEN td.transaction_type = 'DEPOSIT' THEN td.amount END) as avg_deposit_amount,
        AVG(CASE WHEN td.transaction_type = 'WITHDRAWAL' THEN td.amount END) as avg_withdrawal_amount,
        MAX(CASE WHEN td.transaction_type = 'DEPOSIT' THEN td.amount END) as max_deposit_amount,
        MAX(CASE WHEN td.transaction_type = 'WITHDRAWAL' THEN td.amount END) as max_withdrawal_amount,
        -- Last transaction date across all accounts
        MAX(td.transaction_date) as last_transaction_date,
        -- Current balance (from most recent transaction)
        MAX(CASE WHEN td.transaction_date = (SELECT MAX(td2.transaction_date) 
                                            FROM transaction_data td2 
                                            WHERE td2.account_key = td.account_key) 
                 THEN td.balance END) as current_balance
    FROM base_savings_data bsd
    LEFT JOIN transaction_data td ON bsd.account_key = td.account_key
    GROUP BY bsd.client_id, bsd.age
),

results AS (
    SELECT 
        client_id,
        age,
        age_group,
        total_accounts,
        first_account_creation,
        last_account_creation,
        last_account_closure,
        total_deposits_count as deposits,
        total_withdrawals_count as withdrawals,
        total_deposits_amount,
        total_withdrawals_amount,
        avg_deposit_amount as avg_deposit_amt,
        avg_withdrawal_amount as avg_withdrawal_amt,
        max_deposit_amount as max_deposit_amt,
        max_withdrawal_amount as max_withdrawal_amt,
        last_transaction_date as last_trans,
        current_balance as last_balance,
        DATEDIFF('day', first_account_creation, last_transaction_date) as days_active,
        -- Churn status based on current balance
        CASE WHEN current_balance < 1 THEN 'closed' 
             WHEN current_balance between 1 and 50 THEN 'likely_closed' 
             WHEN current_balance IS NULL THEN NULL
             ELSE 'active' END as churn_status
    FROM client_summary
)
--select count(distinct client_id) from first_eligibility
select 
-- Behavioral segmentation
CASE 
    WHEN days_active <= 1 AND last_balance < 1 THEN 'testers'
    WHEN (monthly_deposit_frequency >= 10 AND monthly_withdrawal_frequency >= 10) 
         OR (monthly_deposit_frequency >= 5 AND monthly_withdrawal_frequency >= 5) 
         OR (monthly_withdrawal_frequency >= 5) 
         THEN 'wallet_users'
    WHEN last_balance > 500 AND deposits > 1 THEN 'ultra_savers'
    ELSE 'savers'
END as user_segment,
SUM(1) as no_users, 
avg(days_active) as avg_days_active, 
avg(withdrawals) as avg_withdrawals, 
avg(deposits) as avg_deposits, 
avg(avg_deposit_amt) as avg_deposit_amount,
sum(total_deposits_amount) as total_deposits, 
sum(total_withdrawals_amount) as total_withdrawals, 
sum(total_deposits_amount) - sum(total_withdrawals_amount) as net_balance,
avg(last_balance) as avg_current_balance,
count(distinct client_id) as unique_clients,
-- Additional frequency metrics for analysis
avg(monthly_deposit_frequency) as avg_monthly_deposit_frequency,
avg(monthly_withdrawal_frequency) as avg_monthly_withdrawal_frequency,
avg(total_accounts) as avg_accounts_per_client
from 
(select * , 
-- Calculate frequency metrics
deposits / NULLIF(GREATEST(DATEDIFF(MONTH, first_account_creation, last_trans), 1), 0) as monthly_deposit_frequency,
withdrawals / NULLIF(GREATEST(DATEDIFF(MONTH, first_account_creation, last_trans), 1), 0) as monthly_withdrawal_frequency
from results)
group by 
CASE 
    WHEN days_active <= 1 AND last_balance < 1 THEN 'testers'
    WHEN (monthly_deposit_frequency >= 10 AND monthly_withdrawal_frequency >= 10) 
         OR (monthly_deposit_frequency >= 5 AND monthly_withdrawal_frequency >= 5) 
         OR (monthly_withdrawal_frequency >= 5) 
         THEN 'wallet_users'
    WHEN last_balance > 500 AND deposits > 1 THEN 'ultra_savers'
    ELSE 'savers'
END
order by no_users desc

