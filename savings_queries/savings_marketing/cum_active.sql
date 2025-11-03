
WITH date_range AS (
    SELECT DATE('{{Range.start}}') AS date
    
    UNION ALL
    
    SELECT DATEADD(DAY, 1, date)
    FROM date_range
    WHERE date < DATE('{{Range.end}}')
),
active_accounts_by_date AS (
    SELECT 
        d.date as report_date,
        COUNT(DISTINCT sa.id) as accounts
    FROM date_range d
    JOIN GHANA_PROD.MAMBU.SAVINGSACCOUNT sa ON 
        -- Account was created on or before this date
        DATE(sa.creationdate) <= d.date
        AND (
            -- Account is still active
            sa.ACCOUNTSTATE = 'ACTIVE'
            OR 
            -- Account was closed AFTER the date we're evaluating
            (sa.ACCOUNTSTATE = 'CLOSED' AND DATE(sa.closeddate) > d.date)
        )
    LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
    LEFT JOIN GHANA_PROD.MAMBU.SAVINGSTRANSACTION st ON st.PARENTACCOUNTKEY = sa.ENCODEDKEY
    LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_id ON 
        st.ENCODEDKEY = wallet_id.PARENTKEY AND 
        wallet_id.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'ACCOUNT_NUMBER_TRANSACTION_CHANN')
    LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_network ON 
        st.ENCODEDKEY = wallet_network.PARENTKEY AND 
        wallet_network.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'NETWORK_TRANSACTION_CHANNEL')
    LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE identifier ON 
        st.ENCODEDKEY = identifier.PARENTKEY AND 
        identifier.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'IDENTIFIER_TRANSACTION_CHANNEL_I')
    WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
        AND (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584', '23346722591', '233257806345') 
             OR cl.MOBILEPHONE1 is null)
        AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584', '23346722591', '233257806345') 
             OR cl.MOBILEPHONE2 is null) 
        AND (wallet_id.value not in ('233552602681', '233243630046', '233245822584', '23346722591', '233257806345') 
             OR wallet_id.value is null)
        AND DATE(sa.creationdate) >= DATE('2025-04-03')
    GROUP BY 1
),

aggregated_active_accounts AS (
    SELECT 
        TO_CHAR(DATE_TRUNC('{{Scale}}', report_date), 'YYYY-MM-DD') AS creation_date,
        MAX(accounts) as acc_active_accounts
    FROM active_accounts_by_date
    GROUP BY DATE_TRUNC('{{Scale}}', report_date)
)

SELECT 
    creation_date,
    acc_active_accounts,
    LAG(acc_active_accounts) OVER (ORDER BY creation_date) as previous_period_accounts,
    CASE 
        WHEN LAG(acc_active_accounts) OVER (ORDER BY creation_date) > 0 
        THEN ((acc_active_accounts - LAG(acc_active_accounts) OVER (ORDER BY creation_date)) / LAG(acc_active_accounts) OVER (ORDER BY creation_date)) * 100
        ELSE NULL
    END as growth_rate_percentage,
    acc_active_accounts - LAG(acc_active_accounts) OVER (ORDER BY creation_date) as absolute_change
FROM aggregated_active_accounts
ORDER BY creation_date;