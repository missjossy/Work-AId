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

all_transactions AS (
    SELECT 
        st.parentaccountkey as account_key,
        st.entrydate as transaction_date,
        st.amount as transaction_amount,
        st."type" as transaction_type,
        ROW_NUMBER() OVER (PARTITION BY st.parentaccountkey ORDER BY st.entrydate ASC) as deposit_sequence
    FROM GHANA_PROD.MAMBU.SAVINGSTRANSACTION st
    WHERE st."type" IN ('DEPOSIT', 'WITHDRAWAL', 'WITHDRAWAL_ADJUSTMENT', 'ADJUSTMENT')
    AND date(st.entrydate) BETWEEN date('2025-04-03') and date('2025-09-30')
),

deposit_transactions AS (
    SELECT 
        account_key,
        transaction_date,
        transaction_amount,
        transaction_type,
        deposit_sequence
    FROM all_transactions
    WHERE transaction_type = 'DEPOSIT'
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

client_financial_summary AS (
    SELECT 
        sa.client_id,
        SUM(CASE WHEN at.transaction_type = 'DEPOSIT' THEN at.transaction_amount ELSE 0 END) as total_deposits,
        SUM(CASE WHEN at.transaction_type IN ('WITHDRAWAL', 'WITHDRAWAL_ADJUSTMENT') THEN at.transaction_amount ELSE 0 END) as total_withdrawals,
        SUM(CASE WHEN at.transaction_type = 'DEPOSIT' THEN at.transaction_amount 
                 WHEN at.transaction_type IN ('WITHDRAWAL', 'WITHDRAWAL_ADJUSTMENT') THEN -at.transaction_amount 
                 ELSE 0 END) as total_balance,
        COUNT(CASE WHEN at.transaction_type = 'DEPOSIT' THEN 1 END) as deposit_count,
        COUNT(CASE WHEN at.transaction_type IN ('WITHDRAWAL', 'WITHDRAWAL_ADJUSTMENT') THEN 1 END) as withdrawal_count
    FROM savings_accounts sa
    LEFT JOIN all_transactions at ON sa.account_key = at.account_key
    GROUP BY sa.client_id
),

client_summary AS (
    SELECT 
        cdt.client_id,
        MIN(cdt.account_creation_date) as first_account_creation,
        MAX(cdt.account_creation_date) as last_account_creation,
        COUNT(DISTINCT cdt.account_id) as total_accounts,
        -- First deposit timing (across all accounts)
        MIN(cdt.first_deposit_date) as first_deposit_ever,
        MIN(cdt.second_deposit_date) as second_deposit_ever,
        -- Days from signup to first deposit
        MIN(cdt.days_signup_to_first_deposit) as days_signup_to_first_deposit,
        -- Days from first to second deposit
        MIN(cdt.days_first_to_second_deposit) as days_first_to_second_deposit,
        -- Flags
        CASE WHEN MIN(cdt.first_deposit_date) IS NOT NULL THEN TRUE ELSE FALSE END as has_first_deposit,
        CASE WHEN MIN(cdt.second_deposit_date) IS NOT NULL THEN TRUE ELSE FALSE END as has_second_deposit,
        CASE WHEN MIN(cdt.days_signup_to_first_deposit) <= 7 THEN 'Early (≤7 days)'
             WHEN MIN(cdt.days_signup_to_first_deposit) <= 30 THEN 'Moderate (8-30 days)'
             WHEN MIN(cdt.days_signup_to_first_deposit) IS NOT NULL THEN 'Late (>30 days)'
             ELSE 'No deposit' END as first_deposit_timing_category,
        CASE WHEN MIN(cdt.days_first_to_second_deposit) <= 7 THEN 'Quick (≤7 days)'
             WHEN MIN(cdt.days_first_to_second_deposit) <= 30 THEN 'Moderate (8-30 days)'
             WHEN MIN(cdt.days_first_to_second_deposit) IS NOT NULL THEN 'Slow (>30 days)'
             ELSE 'No second deposit' END as second_deposit_timing_category,
        -- Financial metrics
        COALESCE(cfs.total_deposits, 0) as total_deposits,
        COALESCE(cfs.total_withdrawals, 0) as total_withdrawals,
        COALESCE(cfs.total_balance, 0) as total_balance,
        COALESCE(cfs.deposit_count, 0) as deposit_count,
        COALESCE(cfs.withdrawal_count, 0) as withdrawal_count
    FROM client_deposit_timing cdt
    LEFT JOIN client_financial_summary cfs ON cdt.client_id = cfs.client_id
    GROUP BY cdt.client_id, cfs.total_deposits, cfs.total_withdrawals, cfs.total_balance, cfs.deposit_count, cfs.withdrawal_count
)

SELECT 
    client_id,
    first_account_creation,
    last_account_creation,
    total_accounts,
    first_deposit_ever,
    second_deposit_ever,
    days_signup_to_first_deposit,
    days_first_to_second_deposit,
    has_first_deposit,
    has_second_deposit,
    first_deposit_timing_category,
    second_deposit_timing_category,
    -- Financial metrics
    total_deposits,
    total_withdrawals,
    total_balance,
    deposit_count,
    withdrawal_count,
    -- Additional timing metrics
    DATEDIFF(day, first_account_creation, first_deposit_ever) as days_account_to_first_deposit,
    DATEDIFF(day, first_deposit_ever, second_deposit_ever) as days_between_first_second_deposits
FROM client_summary
ORDER BY client_id;
