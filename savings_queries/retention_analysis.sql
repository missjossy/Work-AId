-- Comprehensive analysis of client retention patterns and savings behavior
-- Examines relationship between prior Fido app engagement and savings performance

WITH base_savings_clients AS (
    SELECT 
        cl.id as client_id,
        MIN(cl.encodedkey) as client_key,  -- Take any client_key for the client
        MIN(sa.creationdate) as first_savings_date,
        MAX(sa.creationdate) as last_savings_date,
        COUNT(DISTINCT sa.id) as total_savings_accounts,
        SUM(CASE WHEN st."type" = 'DEPOSIT' THEN st.amount ELSE 0 END) as total_deposits,
        SUM(CASE WHEN st."type" = 'WITHDRAWAL' THEN st.amount ELSE 0 END) as total_withdrawals,
        MAX(sa.balance) as current_balance,
        MAX(sa.closeddate) as last_account_closure
    FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa
    LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
    LEFT JOIN GHANA_PROD.MAMBU.SAVINGSTRANSACTION st ON st.PARENTACCOUNTKEY = sa.ENCODEDKEY
    WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
    AND (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
    AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE2 is null) 
    AND date(sa.creationdate) >= date('2025-04-03')
    AND date(sa.creationdate) between '2025-04-03' and '2025-09-30'
    AND 'ACCOUNTSTATE' != 'WITHDRAWN'
    GROUP BY cl.id  -- Group only by client_id to ensure one row per client
),

-- Get loan history for clients
loan_history AS (
    SELECT 
        m.client_id,
        COUNT(DISTINCT m.loan_id) as total_loans,
        MIN(m.disbursementdate) as first_loan_date,
        MAX(m.disbursementdate) as last_loan_date,
        SUM(m.loanamount) as total_loan_amount,
        AVG(m.loanamount) as avg_loan_amount,
        MAX(m.ln) as max_loan_number,
        -- Calculate loan frequency (loans per month)
        CASE 
            WHEN DATEDIFF(day, MIN(m.disbursementdate), MAX(m.disbursementdate)) > 0 
            THEN COUNT(DISTINCT m.loan_id) * 30.0 / DATEDIFF(day, MIN(m.disbursementdate), MAX(m.disbursementdate))
            ELSE 0 
        END as loan_frequency
    FROM GHANA_PROD.ML.LOAN_INFO_TBL m
    WHERE m.disbursementdate IS NOT NULL
    AND m.disbursementdate < (SELECT MIN(first_savings_date) FROM base_savings_clients)
    GROUP BY m.client_id
),

-- Calculate days between loans separately
loan_gaps AS (
    SELECT 
        client_id,
        AVG(days_between_loans) as avg_days_between_loans
    FROM (
        SELECT 
            client_id,
            DATEDIFF(day, LAG(disbursementdate) OVER (PARTITION BY client_id ORDER BY disbursementdate), disbursementdate) as days_between_loans
        FROM GHANA_PROD.ML.LOAN_INFO_TBL
        WHERE disbursementdate IS NOT NULL
        AND disbursementdate < (SELECT MIN(first_savings_date) FROM base_savings_clients)
    ) gaps
    WHERE days_between_loans IS NOT NULL
    GROUP BY client_id
),

-- Get app signup data (simplified)
app_signup AS (
    SELECT 
        u.banking_platform_id as client_id,
        u.created_timestamp as app_signup_date,
        -- Time between app signup and first savings
        DATEDIFF(day, u.created_timestamp, bsc.first_savings_date) as days_to_first_savings
    FROM GHANA_PROD.BANKING_SERVICE."USER" u
    JOIN base_savings_clients bsc ON u.banking_platform_id = bsc.client_id
),

-- Get login activity patterns
login_activity AS (
    SELECT 
        la.client_id,
        COUNT(DISTINCT la.activity_date) as total_login_days,
        MIN(la.activity_date) as first_login_date,
        MAX(la.activity_date) as last_login_date,
        -- Calculate login frequency before savings (per client)
        CASE 
            WHEN DATEDIFF(day, MIN(la.activity_date), bsc.first_savings_date) > 0
            THEN COUNT(DISTINCT la.activity_date) * 30.0 / 
                 DATEDIFF(day, MIN(la.activity_date), bsc.first_savings_date)
            ELSE 0
        END as login_frequency_before_savings
    FROM (
        SELECT DISTINCT
            bu.banking_platform_id as client_id,
            DATE(bn.timestamp) as activity_date
        FROM data.backend_notifications bn
        JOIN banking_service.user bu ON bn.user_identity = bu.id
        WHERE bn."TYPE" = 'BE_AUTHENTICATION_PHONE_VERIFICATION_2'
        AND TO_TIMESTAMP_NTZ(bn.timestamp) >= '2025-01-01'
    ) la
    JOIN base_savings_clients bsc ON la.client_id = bsc.client_id
    WHERE la.activity_date < bsc.first_savings_date
    GROUP BY la.client_id, bsc.first_savings_date
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
        FROM base_savings_clients bsc
        JOIN GHANA_PROD.MAMBU.SAVINGSACCOUNT sa ON sa.ACCOUNTHOLDERKEY = bsc.client_key
        WHERE bsc.total_savings_accounts > 1
    ) gaps
    WHERE days_between_accounts IS NOT NULL
    GROUP BY client_id
),


-- Combine all data
comprehensive_analysis AS (
    SELECT 
        bsc.*,
        -- Retention status
        CASE 
            WHEN bsc.last_account_closure IS NOT NULL 
            AND DATEDIFF(day, bsc.last_account_closure, '2025-09-30') > 25 THEN 'churned'
            WHEN bsc.total_savings_accounts > 1 
            AND EXISTS (
                SELECT 1 FROM account_gaps_calc agc
                WHERE agc.client_id = bsc.client_id AND agc.max_gap > 25
            ) THEN 'churned'
            ELSE 'retained'
        END as retention_status,
        -- Loan data
        COALESCE(lh.total_loans, 0) as total_loans,
        COALESCE(lh.first_loan_date, NULL) as first_loan_date,
        COALESCE(lh.last_loan_date, NULL) as last_loan_date,
        COALESCE(lh.total_loan_amount, 0) as total_loan_amount,
        COALESCE(lh.avg_loan_amount, 0) as avg_loan_amount,
        COALESCE(lh.max_loan_number, 0) as max_loan_number,
        COALESCE(lh.loan_frequency, 0) as loan_frequency,
        COALESCE(lg.avg_days_between_loans, 0) as avg_days_between_loans,
        -- App engagement data
        COALESCE(as_data.days_to_first_savings, 0) as days_to_first_savings,
        COALESCE(la.total_login_days, 0) as total_login_days,
        COALESCE(la.first_login_date, NULL) as first_login_date,
        COALESCE(la.last_login_date, NULL) as last_login_date,
        COALESCE(la.login_frequency_before_savings, 0) as login_frequency_before_savings,
        -- Calculate savings metrics
        CASE WHEN bsc.total_withdrawals > 0 THEN bsc.total_deposits / bsc.total_withdrawals ELSE NULL END as deposit_to_withdrawal_ratio,
        CASE WHEN bsc.total_savings_accounts > 0 THEN bsc.total_deposits / bsc.total_savings_accounts ELSE 0 END as avg_deposits_per_account,
        DATEDIFF(day, bsc.first_savings_date, COALESCE(bsc.last_account_closure, '2025-09-30')) as savings_tenure_days
    FROM base_savings_clients bsc
    LEFT JOIN loan_history lh ON bsc.client_id = lh.client_id
    LEFT JOIN loan_gaps lg ON bsc.client_id = lg.client_id
    LEFT JOIN app_signup as_data ON bsc.client_id = as_data.client_id
    LEFT JOIN login_activity la ON bsc.client_id = la.client_id
),

-- Final analysis with segmentations
segments as (SELECT 
    retention_status,
    CASE 
        WHEN total_loans = 0 THEN 'no_loans'
        WHEN total_loans = 1 THEN 'single_loan'
        WHEN total_loans BETWEEN 2 AND 3 THEN 'low_frequency'
        WHEN total_loans BETWEEN 4 AND 6 THEN 'medium_frequency'
        ELSE 'high_frequency'
    END as loan_frequency_segment,
    CASE 
        WHEN login_frequency_before_savings = 0 THEN 'no_logins'
        WHEN login_frequency_before_savings < 2 THEN 'low_engagement'
        WHEN login_frequency_before_savings BETWEEN 2 AND 5 THEN 'medium_engagement'
        ELSE 'high_engagement'
    END as engagement_segment,
    -- Metrics
    COUNT(*) as client_count,
    AVG(total_savings_accounts) as avg_savings_accounts,
    AVG(total_deposits) as avg_total_deposits,
    AVG(total_withdrawals) as avg_total_withdrawals,
    AVG(current_balance) as avg_current_balance,
    AVG(deposit_to_withdrawal_ratio) as avg_deposit_withdrawal_ratio,
    AVG(savings_tenure_days) as avg_savings_tenure_days,
    AVG(total_loans) as avg_total_loans,
    AVG(loan_frequency) as avg_loan_frequency,
    AVG(login_frequency_before_savings) as avg_login_frequency,
    AVG(days_to_first_savings) as avg_days_to_first_savings,
    -- Percentiles for key metrics
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_deposits) as median_total_deposits,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY current_balance) as median_current_balance,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY loan_frequency) as median_loan_frequency
FROM comprehensive_analysis
GROUP BY 
    retention_status,
    CASE 
        WHEN total_loans = 0 THEN 'no_loans'
        WHEN total_loans = 1 THEN 'single_loan'
        WHEN total_loans BETWEEN 2 AND 3 THEN 'low_frequency'
        WHEN total_loans BETWEEN 4 AND 6 THEN 'medium_frequency'
        ELSE 'high_frequency'
    END,
    CASE 
        WHEN login_frequency_before_savings = 0 THEN 'no_logins'
        WHEN login_frequency_before_savings < 2 THEN 'low_engagement'
        WHEN login_frequency_before_savings BETWEEN 2 AND 5 THEN 'medium_engagement'
        ELSE 'high_engagement'
    END
ORDER BY 
    retention_status,
    loan_frequency_segment,
    engagement_segment)
    
    select retention_status, engagement_segment, sum(client_count)
    from segments group by 1,2
    order by 1,2;
