-- Analysis of withdrawal frequency patterns and churn behavior
-- Helps justify the need for savings goals feature by showing if frequent withdrawal users churn more

WITH base_savings_data AS (
    SELECT DISTINCT
        cl.id as client_id, 
        cl.encodedkey as client_key,
        sa.id as account_id,
        sa.encodedkey as account_key,
        sa.creationdate as creation_date,
        sa.closeddate,
        sa.accountstate,
        sa.balance
    FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa
    LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
    WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
    AND (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
    AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE2 is null) 
    AND date(sa.creationdate) >= date('2025-04-03')
    AND date(sa.creationdate) between '2025-04-03' and '2025-09-30'
),

-- Get withdrawal transaction data
withdrawal_transactions AS (
    SELECT 
        st.PARENTACCOUNTKEY as account_key,
        st.ENTRYDATE as transaction_date,
        st.amount as withdrawal_amount,
        st."type" as transaction_type,
        wallet_id.value as wallet_id,
        wallet_network.value as network,
        identifier.value as identifier
    FROM GHANA_PROD.MAMBU.SAVINGSTRANSACTION st
    LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_id ON st.ENCODEDKEY = wallet_id.PARENTKEY 
        AND wallet_id.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'ACCOUNT_NUMBER_TRANSACTION_CHANN')
    LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_network ON st.ENCODEDKEY = wallet_network.PARENTKEY 
        AND wallet_network.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'NETWORK_TRANSACTION_CHANNEL')
    LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE identifier ON st.ENCODEDKEY = identifier.PARENTKEY 
        AND identifier.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'IDENTIFIER_TRANSACTION_CHANNEL_I')
    WHERE st."type" IN ('WITHDRAWAL', 'WITHDRAWAL_ADJUSTMENT')
    AND (wallet_id.value not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or wallet_id.value is null)
    AND date(st.ENTRYDATE) >= '2025-04-03'
),

-- Calculate withdrawal patterns per client
withdrawal_patterns AS (
    SELECT 
        bsd.client_id,
        bsd.client_key,
        MIN(bsd.creation_date) as first_savings_date,
        MAX(bsd.creation_date) as last_savings_date,
        MAX(bsd.closeddate) as last_account_closure,
        COUNT(DISTINCT bsd.account_id) as total_savings_accounts,
        -- Withdrawal metrics
        COUNT(DISTINCT wt.transaction_date) as withdrawal_days,
        COUNT(wt.account_key) as total_withdrawals,
        SUM(wt.withdrawal_amount) as total_withdrawal_amount,
        AVG(wt.withdrawal_amount) as avg_withdrawal_amount,
        MAX(wt.withdrawal_amount) as max_withdrawal_amount,
        MIN(wt.transaction_date) as first_withdrawal_date,
        MAX(wt.transaction_date) as last_withdrawal_date,
        -- Calculate withdrawal frequency (withdrawals per month)
        CASE 
            WHEN DATEDIFF(day, MIN(bsd.creation_date), COALESCE(MAX(wt.transaction_date), MAX(bsd.creation_date))) > 0 
            THEN COUNT(wt.account_key) * 30.0 / 
                 DATEDIFF(day, MIN(bsd.creation_date), COALESCE(MAX(wt.transaction_date), MAX(bsd.creation_date)))
            ELSE 0 
        END as withdrawal_frequency,
        -- Calculate withdrawal intensity (withdrawals per active day)
        CASE 
            WHEN COUNT(DISTINCT wt.transaction_date) > 0 
            THEN COUNT(wt.account_key) * 1.0 / COUNT(DISTINCT wt.transaction_date)
            ELSE 0 
        END as withdrawals_per_active_day,
        -- Calculate withdrawal consistency (days between withdrawals)
        CASE 
            WHEN COUNT(wt.account_key) > 1 
            THEN DATEDIFF(day, MIN(wt.transaction_date), MAX(wt.transaction_date)) * 1.0 / (COUNT(wt.account_key) - 1)
            ELSE NULL 
        END as avg_days_between_withdrawals,
        -- Calculate withdrawal velocity (withdrawals in first 30 days)
        COUNT(CASE WHEN wt.transaction_date <= DATEADD(day, 30, first_savings_date) THEN 1 END) as withdrawals_first_30_days,
        -- Calculate withdrawal recency (days since last withdrawal)
        DATEDIFF(day, MAX(wt.transaction_date), '2025-09-30') as days_since_last_withdrawal
    FROM base_savings_data bsd
    LEFT JOIN withdrawal_transactions wt ON bsd.account_key = wt.account_key
    GROUP BY bsd.client_id  -- Group only by client_id to ensure one row per client
),

-- Calculate account gaps for multi-account clients
account_gaps_calc AS (
    SELECT 
        client_id,
        MAX(days_between_accounts) as max_gap
    FROM (
        SELECT 
            bsd.client_id,
            DATEDIFF(day, LAG(bsd.creation_date) OVER (PARTITION BY bsd.client_id ORDER BY bsd.creation_date), bsd.creation_date) as days_between_accounts
        FROM base_savings_data bsd
        WHERE bsd.client_id IN (
            SELECT client_id FROM withdrawal_patterns WHERE total_savings_accounts > 1
        )
    ) gaps
    WHERE days_between_accounts IS NOT NULL
    GROUP BY client_id
),

-- Classify clients as churned or retained
client_retention AS (
    SELECT 
        wp.*,
        CASE 
            WHEN wp.last_account_closure IS NOT NULL 
            AND DATEDIFF(day, wp.last_account_closure, '2025-09-30') > 25 THEN 'churned'
            WHEN wp.total_savings_accounts > 1 
            AND EXISTS (
                SELECT 1 FROM account_gaps_calc agc
                WHERE agc.client_id = wp.client_id AND agc.max_gap > 25
            ) THEN 'churned'
            ELSE 'retained'
        END as retention_status
    FROM withdrawal_patterns wp
),

-- Create withdrawal behavior segments
withdrawal_segments AS (
    SELECT 
        *,
        -- Withdrawal frequency segments
        CASE 
            WHEN withdrawal_frequency = 0 THEN 'no_withdrawals'
            WHEN withdrawal_frequency < 1 THEN 'low_frequency'
            WHEN withdrawal_frequency BETWEEN 1 AND 3 THEN 'medium_frequency'
            WHEN withdrawal_frequency BETWEEN 3 AND 6 THEN 'high_frequency'
            ELSE 'very_high_frequency'
        END as frequency_segment,
        -- Withdrawal intensity segments
        CASE 
            WHEN withdrawals_per_active_day = 0 THEN 'no_withdrawals'
            WHEN withdrawals_per_active_day < 1.5 THEN 'low_intensity'
            WHEN withdrawals_per_active_day BETWEEN 1.5 AND 3 THEN 'medium_intensity'
            ELSE 'high_intensity'
        END as intensity_segment,
        -- Withdrawal consistency segments
        CASE 
            WHEN avg_days_between_withdrawals IS NULL THEN 'no_withdrawals'
            WHEN avg_days_between_withdrawals < 7 THEN 'very_frequent'
            WHEN avg_days_between_withdrawals BETWEEN 7 AND 14 THEN 'frequent'
            WHEN avg_days_between_withdrawals BETWEEN 14 AND 30 THEN 'moderate'
            ELSE 'infrequent'
        END as consistency_segment,
        -- Early withdrawal behavior
        CASE 
            WHEN withdrawals_first_30_days = 0 THEN 'no_early_withdrawals'
            WHEN withdrawals_first_30_days = 1 THEN 'single_early_withdrawal'
            WHEN withdrawals_first_30_days BETWEEN 2 AND 5 THEN 'moderate_early_withdrawals'
            ELSE 'high_early_withdrawals'
        END as early_behavior_segment
    FROM client_retention
)

-- Final analysis: Withdrawal patterns and churn
SELECT 
    retention_status,
    frequency_segment,
    intensity_segment,
    consistency_segment,
    early_behavior_segment,
    -- Client counts and percentages
    COUNT(*) as client_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY retention_status), 2) as pct_within_retention_status,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY frequency_segment), 2) as pct_within_frequency_segment,
    -- Withdrawal behavior metrics
    AVG(withdrawal_frequency) as avg_withdrawal_frequency,
    AVG(withdrawals_per_active_day) as avg_withdrawals_per_active_day,
    AVG(avg_days_between_withdrawals) as avg_days_between_withdrawals,
    AVG(withdrawals_first_30_days) as avg_early_withdrawals,
    AVG(days_since_last_withdrawal) as avg_days_since_last_withdrawal,
    -- Financial metrics
    AVG(total_withdrawal_amount) as avg_total_withdrawal_amount,
    AVG(avg_withdrawal_amount) as avg_avg_withdrawal_amount,
    AVG(max_withdrawal_amount) as avg_max_withdrawal_amount,
    -- Account metrics
    AVG(total_savings_accounts) as avg_total_accounts,
    AVG(withdrawal_days) as avg_withdrawal_days,
    -- Percentiles for key metrics
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY withdrawal_frequency) as median_withdrawal_frequency,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_withdrawal_amount) as median_total_withdrawal_amount,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_days_between_withdrawals) as median_days_between_withdrawals
FROM withdrawal_segments
GROUP BY 
    retention_status,
    frequency_segment,
    intensity_segment,
    consistency_segment,
    early_behavior_segment
ORDER BY 
    retention_status,
    frequency_segment,
    intensity_segment;
