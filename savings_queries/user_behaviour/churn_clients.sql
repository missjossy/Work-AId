WITH base_savings_data AS (
    SELECT DISTINCT
        cl.id as client_id, 
        sa.id as account_id,
        sa.creationdate as creation_date,
        sa.closeddate,
        sa.accountstate,
        sa.encodedkey as account_key
    FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa
    LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
    WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
    AND (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
    AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE2 is null) 
    AND date(sa.creationdate) >= date('2025-04-03')
    AND date(sa.creationdate) between '2025-04-03' and '2025-09-30'
),
account_gaps AS (
    SELECT 
        client_id,
        account_id,
        creation_date,
        closeddate,
        account_key,
        LAG(creation_date) OVER (PARTITION BY client_id ORDER BY creation_date) as previous_creation_date,
        DATEDIFF(day, LAG(creation_date) OVER (PARTITION BY client_id ORDER BY creation_date), creation_date) as days_since_previous_account,
        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY creation_date DESC) as account_rank
    FROM base_savings_data
),
transaction_data AS (
    SELECT 
        st.parentaccountkey as account_key,
        st.creationdate as transaction_date,
        st.amount,
        st."type" as transaction_type,
        st.balance
    FROM GHANA_PROD.MAMBU.SAVINGSTRANSACTION st
    WHERE st."type" IN ('WITHDRAWAL', 'DEPOSIT')
),
prev_month_balances AS (
    SELECT 
        account_key,
        DATE_TRUNC('month', closure_date) as closure_month,
        balance as month_start_balance
    FROM (
        SELECT 
            td.account_key,
            DATE_TRUNC('month', ag.closeddate) as closure_date,
            td.balance,
            ROW_NUMBER() OVER (PARTITION BY td.account_key, DATE_TRUNC('month', ag.closeddate) ORDER BY td.transaction_date DESC) as rn
        FROM transaction_data td
        JOIN account_gaps ag ON td.account_key = ag.account_key
        WHERE td.transaction_date < DATE_TRUNC('month', ag.closeddate)
        AND ag.closeddate IS NOT NULL
    ) ranked
    WHERE rn = 1
),
monthly_balances AS (
    SELECT 
        ag.client_id,
        ag.account_id,
        ag.closeddate,
        ag.account_key,
        DATE_TRUNC('month', ag.closeddate) as closure_month,
        COALESCE(pmb.month_start_balance, 0) as month_start_balance,
        COUNT(CASE WHEN td.transaction_type = 'WITHDRAWAL' THEN 1 END) as withdrawals_in_month
    FROM account_gaps ag
    LEFT JOIN transaction_data td ON ag.account_key = td.account_key
    AND DATE_TRUNC('month', td.transaction_date) = DATE_TRUNC('month', ag.closeddate)
    LEFT JOIN prev_month_balances pmb ON ag.account_key = pmb.account_key 
    AND DATE_TRUNC('month', ag.closeddate) = pmb.closure_month
    WHERE ag.closeddate IS NOT NULL
    GROUP BY ag.client_id, ag.account_id, ag.closeddate, ag.account_key, DATE_TRUNC('month', ag.closeddate), pmb.month_start_balance
),
multi_account_churned AS (
    SELECT 
        client_id,
        MAX(days_since_previous_account) as max_gap_days
    FROM account_gaps
    WHERE days_since_previous_account IS NOT NULL
    GROUP BY client_id
    HAVING MAX(days_since_previous_account) > 25
),
single_account_churned AS (
    SELECT 
        client_id,
        account_id,
        creation_date,
        closeddate,
        DATEDIFF(day, closeddate, '2025-09-30') as days_since_closed
    FROM base_savings_data
    WHERE closeddate IS NOT NULL
    AND DATEDIFF(day, closeddate, '2025-09-30') > 25
    AND client_id NOT IN (SELECT client_id FROM multi_account_churned)
),
all_churned_clients AS (
    SELECT client_id FROM multi_account_churned
    UNION
    SELECT client_id FROM single_account_churned
),
client_lifespans AS (
    SELECT 
        client_id,
        MIN(creation_date) as first_account_creation,
        MAX(closeddate) as last_account_closure,
        DATEDIFF(day, MIN(creation_date), MAX(closeddate)) as active_period_days
    FROM account_gaps
    WHERE client_id IN (SELECT client_id FROM all_churned_clients)
    AND closeddate IS NOT NULL
    GROUP BY client_id
)
SELECT 
    ag.client_id,
    -- Client flags
    CASE 
        WHEN ag.client_id IN (SELECT client_id FROM all_churned_clients) THEN TRUE
        ELSE FALSE
    END as is_churned,
    CASE 
        WHEN MAX(CASE WHEN ag.account_rank = 1 THEN ag.closeddate END) IS NULL THEN TRUE  -- Most recent account still active
        WHEN DATEDIFF(day, MAX(CASE WHEN ag.account_rank = 1 THEN ag.closeddate END), '2025-09-30') < 25 THEN TRUE  -- Most recent account closed < 25 days ago
        ELSE FALSE  -- Most recent account closed >= 25 days ago
    END as is_returned,
    -- Account summary
    COUNT(DISTINCT ag.account_id) as total_accounts,
    MIN(ag.creation_date) as first_account_creation,
    MAX(ag.creation_date) as last_account_creation,
    -- Get closure date of the most recent account (by creation date)
    MAX(CASE WHEN ag.account_rank = 1 THEN ag.closeddate END) as last_account_closure,
    -- Churn indicators
    COALESCE(mac.max_gap_days, sac.days_since_closed) as churn_indicator_days,
    CASE 
        WHEN mac.max_gap_days IS NOT NULL THEN 'multi_account_gap'
        WHEN sac.days_since_closed IS NOT NULL THEN 'single_account_closure'
        ELSE 'retained'
    END as churn_type,
    -- Financial summary
    SUM(COALESCE(mb.month_start_balance, 0)) as total_month_start_balance,
    SUM(COALESCE(mb.withdrawals_in_month, 0)) as total_withdrawals_in_closure_months,
    AVG(COALESCE(mb.month_start_balance, 0)) as avg_month_start_balance,
    AVG(COALESCE(mb.withdrawals_in_month, 0)) as avg_withdrawals_in_closure_months,
    -- Client lifespan
    cl.active_period_days,
    DATEDIFF(day, MIN(ag.creation_date), MAX(COALESCE(ag.closeddate, '2025-09-30'))) as total_activity_days
FROM account_gaps ag
LEFT JOIN all_churned_clients ac ON ag.client_id = ac.client_id
LEFT JOIN multi_account_churned mac ON ag.client_id = mac.client_id
LEFT JOIN single_account_churned sac ON ag.client_id = sac.client_id
LEFT JOIN monthly_balances mb ON ag.client_id = mb.client_id AND ag.account_id = mb.account_id
LEFT JOIN client_lifespans cl ON ag.client_id = cl.client_id
GROUP BY 
    ag.client_id,
    COALESCE(mac.max_gap_days, sac.days_since_closed),
    CASE 
        WHEN mac.max_gap_days IS NOT NULL THEN 'multi_account_gap'
        WHEN sac.days_since_closed IS NOT NULL THEN 'single_account_closure'
        ELSE 'retained'
    END,
    cl.active_period_days
ORDER BY ag.client_id;