SELECT 
    client_id,
    first_creation_date,
    current_account_state,
    total_deposits,
    total_withdrawals,
    current_balance,
    total_accounts
FROM (
    SELECT 
        client_id,
        first_creation_date,
        current_account_state,
        total_accounts,
        total_deposits,
        total_withdrawals,
        current_balance
    FROM (
        SELECT 
            cl.id as client_id,
            MIN(sa.creationdate) as first_creation_date,
            COUNT(DISTINCT sa.id) as total_accounts,
            SUM(CASE WHEN st."type" = 'DEPOSIT' THEN st.amount ELSE 0 END) as total_deposits,
            SUM(CASE WHEN st."type" = 'WITHDRAWAL' THEN st.amount ELSE 0 END) as total_withdrawals,
            SUM(CASE WHEN sa.balance IS NOT NULL THEN sa.balance ELSE 0 END) as current_balance
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
        GROUP BY cl.id
    ) aggregated_data
    LEFT JOIN (
        SELECT DISTINCT
            cl.id as client_id,
            FIRST_VALUE(sa.accountstate) OVER(partition by cl.id order by sa.creationdate desc) as current_account_state
        FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa
        LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
        WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
        AND (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
        AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE2 is null) 
        AND date(sa.creationdate) >= date('2025-04-03')
        AND date(sa.creationdate) between '2025-04-03' and '2025-09-23'
        AND accountstate != 'WITHDRAWN'
    ) account_states ON aggregated_data.client_id = account_states.client_id
) client_data
ORDER BY client_id;
