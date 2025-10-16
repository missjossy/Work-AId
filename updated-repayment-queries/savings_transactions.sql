WITH active_users AS (
	SELECT USER.id, BANKING_PLATFORM_ID, USER.NATIONAL_ID
	FROM GHANA_PROD.BANKING_SERVICE.USER 
	WHERE JSON_EXTRACT_PATH_TEXT(status,'status') = 'ACTIVE'
), 
last_survey AS (
	SELECT client_id, EMPLOYMENT, INDUSTRY, INCOME_VALUE , FIRSTNAME, LASTNAME --, *
	FROM GHANA_PROD."DATA".SURVEY_DATA 
	WHERE FIRSTNAME is not null --and LASTNAME is not null
	QUALIFY row_number() OVER (PARTITION BY client_id ORDER BY loan_date desc)=1 
	
),
payment_details as(
select created_on as transaction_date,amount, transaction_type, 
PARSE_JSON(payment_details):banking_platform_id::string as client_id,
PARSE_JSON(payment_details):walletId::string as mobilephone,
from money_transfer.transaction_metadata_p 
where requester_id = 'savings-service'
and date(created_on) >= '2025-04-03'
),

-- All transactions from ghana_prod.savings.savings_transactions (includes failed, pending, successful)
savings_transactions_all AS (
    SELECT 
        ss.ID,
        ss.created_on AS date_and_time,
        ss.client_id,
        ss.account_id,
        ss.ID as transactionid,
        ss.transaction_type,
        ss.amount AS transaction_amount,
        ss."EXTERNAL_ID" as momo_id,
        ss.state as transaction_status,
        ss.wallet_id,
        ' ' as encodedkey,
        'savings_transactions' as source_table
    FROM ghana_prod.savings.savings_transactions ss
    WHERE date(ss.created_on) BETWEEN '2025-05-01' and '2025-05-27'
    AND date(ss.created_on) >= '2025-03-01'
    AND ss.transaction_type in ('DEPOSIT','WITHDRAWAL', 'WITHDRAWAL_ADJUSTMENT','ADJUSTMENT')
),

-- All transactions from mambu.savingstransaction (successful and system transactions)
mambu_transactions_all AS (
    SELECT 
        TRY_CAST(cv.value AS INTEGER) as ID,
        st.ENTRYDATE as date_and_time,
        c.ID as client_id,
        sa.ID as account_id,
        st.TRANSACTIONID,
        st."type" AS transaction_type,
        st.AMOUNT AS transaction_amount,
        ' ' as momo_id,
        NULL as transaction_status, -- System transactions have no state
        NULL as wallet_id,
        st.ENCODEDKEY,
        'mambu_transactions' as source_table
    FROM mambu.savingstransaction st
    LEFT JOIN GHANA_PROD.MAMBU.SAVINGSACCOUNT sa ON st.PARENTACCOUNTKEY = sa.ENCODEDKEY
    LEFT JOIN GHANA_PROD.MAMBU.CLIENT c ON sa.ACCOUNTHOLDERKEY = c.ENCODEDKEY
    LEFT JOIN ghana_prod.MAMBU.customfieldvalue cv ON cv.PARENTKEY = st.encodedkey 
        AND cv.CUSTOMFIELDKEY IN (
            SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'INTERNAL_ID'
        )
    WHERE date(st.ENTRYDATE) BETWEEN '2025-05-01' and '2025-05-27'
    AND date(st.ENTRYDATE) >= '2025-03-01'
    AND st."type" in ('WITHDRAWAL_ADJUSTMENT','ADJUSTMENT')
    AND sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
    AND cv.value IS NOT NULL
),

-- Combined transactions
all_transactions AS (
    SELECT * FROM savings_transactions_all
    UNION ALL
    SELECT * FROM mambu_transactions_all
)

SELECT DISTINCT 
       at.date_and_time,
       at.client_id,
       at.account_id,
       COALESCE(ls.FIRSTNAME||' ' || coalesce(ls.LASTNAME, ''), c.FIRSTNAME|| ' '|| coalesce(c.LASTNAME, ' ')) as client_name, 
       ba.ACCOUNT_NUMBER,
       au.NATIONAL_ID AS id_number,
       at.transactionid,
       at.transaction_type,
       at.transaction_amount,
       wallet_id.value AS wallet_number,
       wallet_network.value AS NETWORK,
       at.momo_id, 
       at.transaction_status,
       identifier.value AS identifier,
       at.source_table
FROM all_transactions at
LEFT JOIN GHANA_PROD.MAMBU.SAVINGSACCOUNT sa ON at.account_id = sa.id
LEFT JOIN GHANA_PROD.MAMBU.CLIENT c ON c.ID = at.client_id
LEFT JOIN active_users au ON au.BANKING_PLATFORM_ID = c.id
LEFT JOIN last_survey ls on ls.client_id = c.id
LEFT JOIN ghana_prod.savings.bank_account ba on ba.client_id = c.id
LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_id ON at.encodedkey = wallet_id.PARENTKEY AND wallet_id.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'ACCOUNT_NUMBER_TRANSACTION_CHANN')
LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_network ON at.encodedkey = wallet_network.PARENTKEY AND wallet_network.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'NETWORK_TRANSACTION_CHANNEL')
LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE identifier ON at.encodedkey = identifier.PARENTKEY AND identifier.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'IDENTIFIER_TRANSACTION_CHANNEL_I')
WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS' 
AND (c.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or c.MOBILEPHONE1 is null)
AND (c.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or c.MOBILEPHONE2 is null) 
AND (wallet_id.value not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or wallet_id.value is null)
ORDER BY at.date_and_time DESC;