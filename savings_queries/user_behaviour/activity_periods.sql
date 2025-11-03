WITH deposits_over_time AS (
    SELECT 
        day(st.entrydate) as day_of_month, 
        sum(st.amount) AS deposits_amount, 
        count(case when st."type" = 'DEPOSIT' then 1 end) as deposits_count
    FROM GHANA_PROD.MAMBU.SAVINGSTRANSACTION st
    LEFT JOIN GHANA_PROD.MAMBU.SAVINGSACCOUNT sa ON st.PARENTACCOUNTKEY = sa.ENCODEDKEY
    LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
    LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_id ON st.ENCODEDKEY = wallet_id.PARENTKEY AND wallet_id.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'ACCOUNT_NUMBER_TRANSACTION_CHANN')
    WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
    AND (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
    AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE2 is null)
    AND (wallet_id.value not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or wallet_id.value is null)
    AND st."type" = 'DEPOSIT'
    AND date(st.entrydate) BETWEEN '{{Range.start}}' and '{{Range.end}}'
    GROUP BY 1
),

accounts_created_over_time AS (
    SELECT 
        day(sa.creationdate) as day_of_month,
        count(DISTINCT sa.id) as accounts_created_count,
        count(DISTINCT cl.id) as clients_created_count
    FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa
    LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
    LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_id ON sa.ENCODEDKEY = wallet_id.PARENTKEY AND wallet_id.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'ACCOUNT_NUMBER_TRANSACTION_CHANN')
    WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
    AND (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
    AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE2 is null)
    AND (wallet_id.value not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or wallet_id.value is null)
    AND date(sa.creationdate) BETWEEN '{{Range.start}}' and '{{Range.end}}'
    GROUP BY 1
),

-- First account creation by day of month
first_account_creation AS (
    SELECT 
        day(first_creation_date) as day_of_month,
        count(DISTINCT client_id) as first_time_clients_count
    FROM (
        SELECT 
            cl.id as client_id,
            min(sa.creationdate) as first_creation_date
        FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa
        LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
        LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_id ON sa.ENCODEDKEY = wallet_id.PARENTKEY AND wallet_id.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'ACCOUNT_NUMBER_TRANSACTION_CHANN')
        WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
        AND (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
        AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE2 is null)
        AND (wallet_id.value not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or wallet_id.value is null)
        AND date(sa.creationdate) BETWEEN '{{Range.start}}' and '{{Range.end}}'
        GROUP BY cl.id
    ) first_accounts
    GROUP BY 1
),

-- Generate all days of the month (1-31) for complete chart
all_days AS (
    SELECT seq4() + 1 as day_of_month
    FROM table(generator(rowcount => 31))
)

SELECT 
    ad.day_of_month,
    COALESCE(dot.deposits_amount, 0) as deposits_amount,
    COALESCE(dot.deposits_count, 0) as deposits_count,
    COALESCE(acot.accounts_created_count, 0) as accounts_created_count,
    COALESCE(acot.clients_created_count, 0) as clients_created_count,
    COALESCE(fac.first_time_clients_count, 0) as first_time_clients_count
FROM all_days ad
LEFT JOIN deposits_over_time dot ON ad.day_of_month = dot.day_of_month
LEFT JOIN accounts_created_over_time acot ON ad.day_of_month = acot.day_of_month
LEFT JOIN first_account_creation fac ON ad.day_of_month = fac.day_of_month
ORDER BY ad.day_of_month