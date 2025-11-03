savings_data as (
select  cl.id as client_id, 
    min(sa.creationdate) OVER(partition by cl.id) as first_creation_date,
    sa.id as account_id , sa.creationdate
        FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa
        LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
        LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_id ON sa.ENCODEDKEY = wallet_id.PARENTKEY AND wallet_id.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'ACCOUNT_NUMBER_TRANSACTION_CHANN')
        WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
         AND DATE(sa.creationdate) >= date('2025-04-03')
        AND (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
        AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE2 is null)
        AND (wallet_id.value not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or wallet_id.value is null)
        AND sa.accountstate != 'WITHDRAWN'

), 

new_savers AS ( 
SELECT 
date_trunc('{{Scale}}', first_creation_date) date_ , 
count(distinct client_id) as new_daily_clients, 
count(distinct account_id)  as new_daily_accounts 
from savings_data
where date(first_creation_date) BETWEEN date('{{Range.start}}') and date('{{ Range.end }}')
group by date_ 
order by date_), 

returning_savers AS (
SELECT 
date_trunc('{{Scale}}', creationdate) date_ , 
count(distinct client_id) as returning_daily_clients, 
count(distinct account_id)  as returning_daily_accounts 
from savings_data
where date(creationdate) BETWEEN date('{{Range.start}}') and date('{{ Range.end }}')
  AND first_creation_date < creationdate
group by date_ 
order by date_),
