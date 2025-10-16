-- Analysis of payment failure rates and their impact on savings product retention
-- Examines whether payment failures influence client churn behavior

WITH savings_transactions_all AS (
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
    WHERE date(ss.created_on) >= '2025-03-01'
    AND ss.transaction_type in ('DEPOSIT','WITHDRAWAL', 'WITHDRAWAL_ADJUSTMENT','ADJUSTMENT')
    AND ss.state != 'PENDING'
),

mambu_transactions AS (
    SELECT 
        st.ENCODEDKEY as encodedkey,
        st.ENTRYDATE as date_and_time,
        c.id as client_id,
        sa.id as account_id,
        st.TRANSACTIONID as transactionid,
        st."type" as transaction_type,
        st.amount AS transaction_amount,
        cv.value as momo_id,
        'SUCCESS' as transaction_status,
        wallet_id.value as wallet_id,
        'mambu' as source_table
    FROM GHANA_PROD.MAMBU.SAVINGSTRANSACTION st
    LEFT JOIN GHANA_PROD.MAMBU.SAVINGSACCOUNT sa ON st.PARENTACCOUNTKEY = sa.ENCODEDKEY
    LEFT JOIN GHANA_PROD.MAMBU.CLIENT c ON sa.ACCOUNTHOLDERKEY = c.ENCODEDKEY
    LEFT JOIN ghana_prod.MAMBU.customfieldvalue cv ON cv.PARENTKEY = st.encodedkey 
        AND cv.CUSTOMFIELDKEY IN (
            SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'INTERNAL_ID'
        )
    LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_id ON st.ENCODEDKEY = wallet_id.PARENTKEY 
        AND wallet_id.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'ACCOUNT_NUMBER_TRANSACTION_CHANN')
    WHERE st."type" in ('DEPOSIT','WITHDRAWAL', 'WITHDRAWAL_ADJUSTMENT','ADJUSTMENT')
    AND sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
    AND (c.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or c.MOBILEPHONE1 is null)
    AND (c.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or c.MOBILEPHONE2 is null) 
    AND (wallet_id.value not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or wallet_id.value is null)
    AND date(st.ENTRYDATE) >= '2025-03-01'
),

-- Combine all transactions
all_transactions AS (
    SELECT * FROM savings_transactions_all
    UNION ALL
    SELECT 
        encodedkey,
        date_and_time,
        client_id,
        account_id,
        transactionid,
        transaction_type,
        transaction_amount,
        momo_id,
        transaction_status,
        wallet_id,
        encodedkey,
        source_table
    FROM mambu_transactions
),

-- Get client savings account information
savings_clients AS (
    SELECT DISTINCT
        cl.id as client_id,
        cl.encodedkey as client_key,
        MIN(sa.creationdate) as first_savings_date,
        MAX(sa.creationdate) as last_savings_date,
        MAX(sa.closeddate) as last_account_closure,
        COUNT(DISTINCT sa.id) as total_savings_accounts,
        SUM(CASE WHEN st."type" = 'DEPOSIT' THEN st.amount ELSE 0 END) as total_deposits,
        SUM(CASE WHEN st."type" = 'WITHDRAWAL' THEN st.amount ELSE 0 END) as total_withdrawals,
        MAX(sa.balance) as current_balance
    FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa
    LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
    LEFT JOIN GHANA_PROD.MAMBU.SAVINGSTRANSACTION st ON st.PARENTACCOUNTKEY = sa.ENCODEDKEY
    WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
    AND (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
    AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE2 is null) 
    AND date(sa.creationdate) >= date('2025-04-03')
    AND date(sa.creationdate) between '2025-04-03' and '2025-09-30'
    GROUP BY cl.id, cl.encodedkey
),

-- Calculate payment failure rates per client
payment_failure_analysis AS (
    SELECT 
        client_id,
        COUNT(*) as total_transactions,
        SUM(CASE WHEN transaction_status = 'SUCCESSFUL' THEN 1 ELSE 0 END) as successful_transactions,
        SUM(CASE WHEN transaction_status = 'FAILED' THEN 1 ELSE 0 END) as failed_transactions,
        SUM(CASE WHEN transaction_status = 'SUCCESSFUL' THEN transaction_amount ELSE 0 END) as successful_amount,
        SUM(CASE WHEN transaction_status = 'FAILED' THEN transaction_amount ELSE 0 END) as failed_amount,
        -- Calculate failure rates
        CASE 
            WHEN COUNT(*) > 0 THEN 
                SUM(CASE WHEN transaction_status = 'FAILED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
            ELSE 0 
        END as transaction_failure_rate,
        CASE 
            WHEN SUM(CASE WHEN transaction_status = 'SUCCESSFUL' THEN transaction_amount ELSE 0 END) + 
                 SUM(CASE WHEN transaction_status = 'FAILED' THEN transaction_amount ELSE 0 END) > 0 THEN
                SUM(CASE WHEN transaction_status = 'FAILED' THEN transaction_amount ELSE 0 END) * 100.0 / 
                (SUM(CASE WHEN transaction_status = 'SUCCESSFUL' THEN transaction_amount ELSE 0 END) + 
                 SUM(CASE WHEN transaction_status = 'FAILED' THEN transaction_amount ELSE 0 END))
            ELSE 0
        END as amount_failure_rate,
        -- Transaction frequency
        COUNT(DISTINCT DATE(date_and_time)) as active_days,
        MIN(date_and_time) as first_transaction_date,
        MAX(date_and_time) as last_transaction_date
    FROM all_transactions
    WHERE client_id IS NOT NULL
    GROUP BY client_id
),

-- Calculate account gaps for multi-account clients
account_gaps_calc AS (
    SELECT 
        client_id,
        MAX(days_between_accounts) as max_gap
    FROM (
        SELECT 
            bsc.client_id,
            DATEDIFF(day, LAG(sa.creationdate) OVER (PARTITION BY bsc.client_id ORDER BY sa.creationdate), sa.creationdate) as days_between_accounts
        FROM savings_clients bsc
        JOIN GHANA_PROD.MAMBU.SAVINGSACCOUNT sa ON sa.ACCOUNTHOLDERKEY = bsc.client_key
        WHERE bsc.total_savings_accounts > 1
    ) gaps
    WHERE days_between_accounts IS NOT NULL
    GROUP BY client_id
),

-- Classify clients as churned or retained
client_retention AS (
    SELECT 
        sc.*,
        CASE 
            WHEN sc.last_account_closure IS NOT NULL 
            AND DATEDIFF(day, sc.last_account_closure, '2025-09-30') > 25 THEN 'churned'
            WHEN sc.total_savings_accounts > 1 
            AND EXISTS (
                SELECT 1 FROM account_gaps_calc agc
                WHERE agc.client_id = sc.client_id AND agc.max_gap > 25
            ) THEN 'churned'
            ELSE 'retained'
        END as retention_status
    FROM savings_clients sc
),

-- Combine retention and payment failure data
comprehensive_analysis AS (
    SELECT 
        cr.*,
        COALESCE(pfa.total_transactions, 0) as total_transactions,
        COALESCE(pfa.successful_transactions, 0) as successful_transactions,
        COALESCE(pfa.failed_transactions, 0) as failed_transactions,
        COALESCE(pfa.successful_amount, 0) as successful_amount,
        COALESCE(pfa.failed_amount, 0) as failed_amount,
        COALESCE(pfa.transaction_failure_rate, 0) as transaction_failure_rate,
        COALESCE(pfa.amount_failure_rate, 0) as amount_failure_rate,
        COALESCE(pfa.active_days, 0) as active_days,
        COALESCE(pfa.first_transaction_date, NULL) as first_transaction_date,
        COALESCE(pfa.last_transaction_date, NULL) as last_transaction_date,
        -- Calculate savings metrics
        CASE WHEN cr.total_withdrawals > 0 THEN cr.total_deposits / cr.total_withdrawals ELSE NULL END as deposit_to_withdrawal_ratio,
        CASE WHEN cr.total_savings_accounts > 0 THEN cr.total_deposits / cr.total_savings_accounts ELSE 0 END as avg_deposits_per_account,
        DATEDIFF(day, cr.first_savings_date, COALESCE(cr.last_account_closure, '2025-09-30')) as savings_tenure_days
    FROM client_retention cr
    LEFT JOIN payment_failure_analysis pfa ON cr.client_id = pfa.client_id
)

-- Final analysis: Payment failure impact on retention
SELECT 
    retention_status,
    CASE 
        WHEN transaction_failure_rate = 0 THEN 'no_failures'
        WHEN transaction_failure_rate < 5 THEN 'low_failure_rate'
        WHEN transaction_failure_rate BETWEEN 5 AND 15 THEN 'medium_failure_rate'
        WHEN transaction_failure_rate BETWEEN 15 AND 30 THEN 'high_failure_rate'
        ELSE 'very_high_failure_rate'
    END as failure_rate_segment,
    CASE 
        WHEN total_transactions = 0 THEN 'no_transactions'
        WHEN total_transactions BETWEEN 1 AND 5 THEN 'low_activity'
        WHEN total_transactions BETWEEN 6 AND 20 THEN 'medium_activity'
        ELSE 'high_activity'
    END as transaction_activity_segment,
    -- Client counts and metrics
    COUNT(*) as client_count,
    AVG(transaction_failure_rate) as avg_transaction_failure_rate,
    AVG(amount_failure_rate) as avg_amount_failure_rate,
    AVG(total_transactions) as avg_total_transactions,
    AVG(failed_transactions) as avg_failed_transactions,
    AVG(successful_transactions) as avg_successful_transactions,
    -- Savings performance metrics
    AVG(total_deposits) as avg_total_deposits,
    AVG(total_withdrawals) as avg_total_withdrawals,
    AVG(current_balance) as avg_current_balance,
    AVG(deposit_to_withdrawal_ratio) as avg_deposit_withdrawal_ratio,
    AVG(savings_tenure_days) as avg_savings_tenure_days,
    -- Percentiles for key metrics
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY transaction_failure_rate) as median_transaction_failure_rate,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_deposits) as median_total_deposits,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY current_balance) as median_current_balance
FROM comprehensive_analysis
GROUP BY 
    retention_status,
    CASE 
        WHEN transaction_failure_rate = 0 THEN 'no_failures'
        WHEN transaction_failure_rate < 5 THEN 'low_failure_rate'
        WHEN transaction_failure_rate BETWEEN 5 AND 15 THEN 'medium_failure_rate'
        WHEN transaction_failure_rate BETWEEN 15 AND 30 THEN 'high_failure_rate'
        ELSE 'very_high_failure_rate'
    END,
    CASE 
        WHEN total_transactions = 0 THEN 'no_transactions'
        WHEN total_transactions BETWEEN 1 AND 5 THEN 'low_activity'
        WHEN total_transactions BETWEEN 6 AND 20 THEN 'medium_activity'
        ELSE 'high_activity'
    END
ORDER BY 
    retention_status,
    failure_rate_segment,
    transaction_activity_segment;
