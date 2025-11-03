-- Active vs Closed Users Over Time Analysis
-- Parameter: {{time_period}} - 'day', 'week', or 'month'
-- Shows active users over time and users who were active but are now closed (churned)

WITH base_savings_data AS (
    SELECT DISTINCT
        cl.id as client_id, 
        sa.id as account_id,
        sa.creationdate as creation_date,
        sa.closeddate,
        sa.accountstate,
        cl.birthdate,
        DATEDIFF('year', cl.birthdate, current_date) as age
    FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa
    LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
    WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
    AND (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
    AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE2 is null) 
    AND date(sa.creationdate) >= date('2025-04-03')
),

-- Get all time periods in our analysis period based on the parameter
all_periods AS (
    SELECT DISTINCT 
        DATE_TRUNC('week', date(creation_date)) as period_start,
        CASE 
            WHEN 'week' = 'day' THEN DATE_TRUNC('day', date(creation_date))
            WHEN 'week' = 'week' THEN DATE_TRUNC('week', date(creation_date)) + INTERVAL '6 days'
            WHEN 'week' = 'month' THEN LAST_DAY(DATE_TRUNC('month', date(creation_date)))
        END as period_end
    FROM base_savings_data
    WHERE date(creation_date) BETWEEN '2025-04-03' AND '2025-10-15'
    
    UNION
    
    SELECT DISTINCT 
        DATE_TRUNC('week', date(closeddate)) as period_start,
        CASE 
            WHEN 'week' = 'day' THEN DATE_TRUNC('day', date(closeddate))
            WHEN 'week' = 'week' THEN DATE_TRUNC('week', date(closeddate)) + INTERVAL '6 days'
            WHEN 'week' = 'month' THEN LAST_DAY(DATE_TRUNC('month', date(closeddate)))
        END as period_end
    FROM base_savings_data
    WHERE closeddate IS NOT NULL
    AND date(closeddate) BETWEEN '2025-04-03' AND '2025-10-15'
),

-- For each period, determine which users were active
period_user_status AS (
    SELECT 
        ap.period_start,
        ap.period_end,
        bsd.client_id,
        -- Was this user active during this period?
        CASE 
            WHEN MIN(bsd.creation_date) <= ap.period_end 
            AND (MAX(bsd.closeddate) IS NULL OR MAX(bsd.closeddate) > ap.period_start)
            THEN 1 
            ELSE 0 
        END as was_active_during_period,
        -- Is this user currently closed?
        CASE 
            WHEN MAX(bsd.closeddate) IS NOT NULL 
            AND MAX(bsd.closeddate) <= CURRENT_DATE
            THEN 1 
            ELSE 0 
        END as is_currently_closed,
        -- User's first account creation
        MIN(bsd.creation_date) as first_account_date,
        -- User's last account closure
        MAX(bsd.closeddate) as last_account_closure,
        -- Total accounts for this user
        COUNT(DISTINCT bsd.account_id) as total_accounts
    FROM all_periods ap
    CROSS JOIN base_savings_data bsd
    GROUP BY ap.period_start, ap.period_end, bsd.client_id
)

-- Final results: Period-based active vs closed analysis
SELECT 
    period_start,
    period_end,
    -- Total active users during this period
    COUNT(DISTINCT CASE WHEN was_active_during_period = 1 THEN client_id END) as total_active_users,
    -- Of those active users, how many are still active today?
    COUNT(DISTINCT CASE WHEN was_active_during_period = 1 AND is_currently_closed = 0 THEN client_id END) as active_users_still_active,
    -- Of those active users, how many have since closed?
    COUNT(DISTINCT CASE WHEN was_active_during_period = 1 AND is_currently_closed = 1 THEN client_id END) as active_users_now_closed,
    -- Percentage breakdown
    ROUND(
        COUNT(DISTINCT CASE WHEN was_active_during_period = 1 AND is_currently_closed = 1 THEN client_id END) * 100.0 / 
        NULLIF(COUNT(DISTINCT CASE WHEN was_active_during_period = 1 THEN client_id END), 0), 2
    ) as pct_active_users_now_closed,
    ROUND(
        COUNT(DISTINCT CASE WHEN was_active_during_period = 1 AND is_currently_closed = 0 THEN client_id END) * 100.0 / 
        NULLIF(COUNT(DISTINCT CASE WHEN was_active_during_period = 1 THEN client_id END), 0), 2
    ) as pct_active_users_still_active,
    -- Churn insights
    ROUND(AVG(CASE WHEN was_active_during_period = 1 AND is_currently_closed = 1 
        THEN DATEDIFF('day', first_account_date, period_end) END), 1) as avg_days_active_before_churn,
    ROUND(AVG(CASE WHEN was_active_during_period = 1 AND is_currently_closed = 1 
        THEN total_accounts END), 1) as avg_accounts_per_churned_user,
    -- Running totals
    SUM(COUNT(DISTINCT CASE WHEN was_active_during_period = 1 THEN client_id END)) OVER (ORDER BY period_start) as cumulative_active_users,
    SUM(COUNT(DISTINCT CASE WHEN was_active_during_period = 1 AND is_currently_closed = 1 THEN client_id END)) OVER (ORDER BY period_start) as cumulative_churned_users
FROM period_user_status
GROUP BY period_start, period_end
ORDER BY period_start;