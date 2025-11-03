SELECT 
    client_id, 
    account_id, 
    creation_date,
    days_since_previous_account,
    days_until_next_account
FROM (
    SELECT 
        client_id,
        account_id,
        creation_date,
        LAG(creation_date) OVER (PARTITION BY client_id ORDER BY creation_date) as previous_creation_date,
        LEAD(creation_date) OVER (PARTITION BY client_id ORDER BY creation_date) as next_creation_date,
        DATEDIFF(day, LAG(creation_date) OVER (PARTITION BY client_id ORDER BY creation_date), creation_date) as days_since_previous_account,
        DATEDIFF(day, creation_date, LEAD(creation_date) OVER (PARTITION BY client_id ORDER BY creation_date)) as days_until_next_account
    FROM (
        SELECT 
            distinct
            cl.id as client_id, 
            sa.id as account_id,
            sa.creationdate as creation_date,
            FIRST_VALUE(sa.accountstate) OVER(partition by cl.id order by sa.creationdate desc) as current_account_state
        FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa
        LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
        LEFT JOIN GHANA_PROD.MAMBU.SAVINGSTRANSACTION st ON st.PARENTACCOUNTKEY = sa.ENCODEDKEY
        LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_id ON st.ENCODEDKEY = wallet_id.PARENTKEY AND wallet_id.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'ACCOUNT_NUMBER_TRANSACTION_CHANN')
        LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_network ON st.ENCODEDKEY = wallet_network.PARENTKEY AND wallet_network.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'NETWORK_TRANSACTION_CHANNEL')
        LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE identifier ON st.ENCODEDKEY = identifier.PARENTKEY AND identifier.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'IDENTIFIER_TRANSACTION_CHANNEL_I')
        WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
        AND (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
        AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE2 is null) 
        AND (wallet_id.value not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or wallet_id.value is null)
        AND date(sa.creationdate) >= date('2025-04-03')
        AND date(sa.creationdate) between '2025-04-03' and '2025-09-23'
        AND accountstate != 'WITHDRAWN'
    ) base_data
) ranked_data
WHERE client_id IN (
    SELECT client_id 
    FROM (
        SELECT client_id, COUNT(DISTINCT account_id) as no_accounts
        FROM (
            SELECT cl.id as client_id, sa.id as account_id
            FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa
            LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
            WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
            AND (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
            AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE2 is null) 
            AND date(sa.creationdate) >= date('2025-04-03')
            AND date(sa.creationdate) between '2025-04-03' and '2025-09-23'
            AND accountstate != 'WITHDRAWN'
        )
        GROUP BY client_id
        HAVING no_accounts > 1
    )
)
ORDER BY client_id, creation_date