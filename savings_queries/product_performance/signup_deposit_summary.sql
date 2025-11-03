WITH app_signup_data AS (
    SELECT 
        u.banking_platform_id as client_id,
        u.created_timestamp as app_signup_date
    FROM GHANA_PROD.BANKING_SERVICE."USER" u
    WHERE u.created_timestamp IS NOT NULL
),

savings_accounts AS (
    SELECT 
        cl.id as client_id,
        sa.id as account_id,
        sa.creationdate as account_creation_date,
        sa.closeddate as account_closed_date,
        sa.accountstate,
        sa.encodedkey as account_key
    FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa
    LEFT JOIN MAMBU.CLIENT cl ON cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
    WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
    AND (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
    AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE2 is null) 
    AND date(sa.creationdate) >= date('2025-04-03')
    AND date(sa.creationdate) between '2025-04-03' and '2025-09-30'
),

deposit_transactions AS (
    SELECT 
        st.parentaccountkey as account_key,
        st.entrydate as transaction_date,
        st.amount as transaction_amount,
        st."type" as transaction_type,
        ROW_NUMBER() OVER (PARTITION BY st.parentaccountkey ORDER BY st.entrydate ASC) as deposit_sequence
    FROM GHANA_PROD.MAMBU.SAVINGSTRANSACTION st
    WHERE st."type" = 'DEPOSIT'
    AND date(st.entrydate) BETWEEN date('2025-04-03') and date('2025-09-30')
),

client_deposit_timing AS (
    SELECT 
        sa.client_id,
        sa.account_id,
        sa.account_creation_date,
        dt.transaction_date as first_deposit_date,
        dt2.transaction_date as second_deposit_date,
        DATEDIFF(day, asd.app_signup_date, dt.transaction_date) as days_signup_to_first_deposit,
        DATEDIFF(day, dt.transaction_date, dt2.transaction_date) as days_first_to_second_deposit
    FROM savings_accounts sa
    LEFT JOIN app_signup_data asd ON sa.client_id = asd.client_id
    LEFT JOIN deposit_transactions dt ON sa.account_key = dt.account_key AND dt.deposit_sequence = 1
    LEFT JOIN deposit_transactions dt2 ON sa.account_key = dt2.account_key AND dt2.deposit_sequence = 2
),

client_summary AS (
    SELECT 
        client_id,
        MIN(account_creation_date) as first_account_creation,
        MIN(first_deposit_date) as first_deposit_ever,
        MIN(second_deposit_date) as second_deposit_ever,
        MIN(days_signup_to_first_deposit) as days_signup_to_first_deposit,
        MIN(days_first_to_second_deposit) as days_first_to_second_deposit,
        CASE WHEN MIN(first_deposit_date) IS NOT NULL THEN TRUE ELSE FALSE END as has_first_deposit,
        CASE WHEN MIN(second_deposit_date) IS NOT NULL THEN TRUE ELSE FALSE END as has_second_deposit
    FROM client_deposit_timing
    GROUP BY client_id
)

SELECT 
    'Overall Statistics' as metric_type,
    COUNT(*) as total_clients,
    COUNT(CASE WHEN has_first_deposit THEN 1 END) as clients_with_first_deposit,
    COUNT(CASE WHEN has_second_deposit THEN 1 END) as clients_with_second_deposit,
    ROUND(COUNT(CASE WHEN has_first_deposit THEN 1 END) * 100.0 / COUNT(*), 2) as first_deposit_rate_pct,
    ROUND(COUNT(CASE WHEN has_second_deposit THEN 1 END) * 100.0 / COUNT(*), 2) as second_deposit_rate_pct,
    ROUND(AVG(days_signup_to_first_deposit), 1) as avg_days_signup_to_first_deposit,
    ROUND(AVG(days_first_to_second_deposit), 1) as avg_days_first_to_second_deposit,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_signup_to_first_deposit), 1) as median_days_signup_to_first_deposit,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_first_to_second_deposit), 1) as median_days_first_to_second_deposit
FROM client_summary

UNION ALL

SELECT 
    'First Deposit Timing' as metric_type,
    COUNT(*) as total_clients,
    NULL as clients_with_first_deposit,
    NULL as clients_with_second_deposit,
    NULL as first_deposit_rate_pct,
    NULL as second_deposit_rate_pct,
    NULL as avg_days_signup_to_first_deposit,
    NULL as avg_days_first_to_second_deposit,
    NULL as median_days_signup_to_first_deposit,
    NULL as median_days_first_to_second_deposit
FROM client_summary
WHERE has_first_deposit = TRUE

UNION ALL

SELECT 
    'Second Deposit Timing' as metric_type,
    COUNT(*) as total_clients,
    NULL as clients_with_first_deposit,
    NULL as clients_with_second_deposit,
    NULL as first_deposit_rate_pct,
    NULL as second_deposit_rate_pct,
    NULL as avg_days_signup_to_first_deposit,
    NULL as avg_days_first_to_second_deposit,
    NULL as median_days_first_to_second_deposit,
    NULL as median_days_first_to_second_deposit
FROM client_summary
WHERE has_second_deposit = TRUE

UNION ALL

SELECT 
    'Early Adopters (≤7 days to first deposit)' as metric_type,
    COUNT(*) as total_clients,
    NULL as clients_with_first_deposit,
    NULL as clients_with_second_deposit,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM client_summary WHERE has_first_deposit = TRUE), 2) as first_deposit_rate_pct,
    NULL as second_deposit_rate_pct,
    NULL as avg_days_signup_to_first_deposit,
    NULL as avg_days_first_to_second_deposit,
    NULL as median_days_signup_to_first_deposit,
    NULL as median_days_first_to_second_deposit
FROM client_summary
WHERE has_first_deposit = TRUE AND days_signup_to_first_deposit <= 7

UNION ALL

SELECT 
    'Quick Second Deposits (≤7 days)' as metric_type,
    COUNT(*) as total_clients,
    NULL as clients_with_first_deposit,
    NULL as clients_with_second_deposit,
    NULL as first_deposit_rate_pct,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM client_summary WHERE has_second_deposit = TRUE), 2) as second_deposit_rate_pct,
    NULL as avg_days_signup_to_first_deposit,
    NULL as avg_days_first_to_second_deposit,
    NULL as median_days_signup_to_first_deposit,
    NULL as median_days_first_to_second_deposit
FROM client_summary
WHERE has_second_deposit = TRUE AND days_first_to_second_deposit <= 7;
