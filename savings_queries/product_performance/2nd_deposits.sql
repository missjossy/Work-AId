with client_base as 
(
select client_id,  last_account_age, last_deposit, --count(distinct account_id) as no_accounts, 
        NTH_VALUE(trans_date, 2) OVER (
                    PARTITION BY client_id
                    ORDER BY trans_date ASC 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) as sec_deposit,
        MIN(case when last_creation_date = creationdate then trans_date end) OVER(partition by client_id order by trans_date) as first_deposit_date,
        datediff('day', first_deposit_date,sec_deposit ) as days_to_2nd

from

(select sa.*, sa.id as account_id, cl.id as client_id, datediff('year', cl.BIRTHDATE, '{{Up To Date}}') as age, st."type" as trans_type ,  
    st.amount as transamount, st.entrydate as trans_date, 
    MAX(CASE WHEN trans_type = 'DEPOSIT' then st.entrydate end) OVER(partition by cl.id) as last_deposit,
    FIRST_VALUE(sa.BALANCE) OVER(partition by cl.id order by sa.creationdate desc) as last_balance,
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
AND date(entrydate) BETWEEN date('2025-04-03') and date('{{Up To Date}}')
AND date(sa.creationdate) >= date('2025-04-03'))

where creationdate = last_creation_date)

select (sum(case when days_to_2nd <= 30 and days_to_2nd is not null then 1 else 0 end ) / count(*))*100 as early_2nd_deposit
from client_base