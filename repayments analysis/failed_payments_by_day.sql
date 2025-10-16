WITH relevant_notifications AS (
    SELECT 
        timestamp,
        "TYPE",
        PARSE_JSON(PAYLOAD):user_id::string as user_id,
        PARSE_JSON(PAYLOAD):client_id::string as client_id,
        PARSE_JSON(PAYLOAD):status::string as status
    FROM data.backend_notifications
    WHERE ("TYPE" = 'BE_AUTHENTICATION_PHONE_VERIFICATION_2' OR "TYPE" = 'BE_LOAN_REPAYMENT_RESULT')
    AND TO_TIMESTAMP_NTZ(timestamp) >= '2024-01-01'
),

daily_payment_attempts AS (
    SELECT 
        DATE(TO_TIMESTAMP_NTZ(timestamp)) as attempt_date,
        DAYOFMONTH(TO_TIMESTAMP_NTZ(timestamp)) as day_of_month,
        "TYPE",
        status,
        COUNT(*) as attempt_count
    FROM relevant_notifications
    WHERE "TYPE" = 'BE_LOAN_REPAYMENT_RESULT'
    GROUP BY 1, 2, 3, 4
),

daily_stats AS (
    SELECT 
        day_of_month,
        SUM(attempt_count) as total_attempts,
        SUM(CASE WHEN status != 'succeeded' THEN attempt_count ELSE 0 END) as failed_attempts
    FROM daily_payment_attempts
    GROUP BY day_of_month
)

SELECT 
    day_of_month,
    total_attempts,
    failed_attempts,
    ROUND(100.0 * failed_attempts / NULLIF(total_attempts, 0), 2) as failure_percentage,
    ROUND(100.0 * total_attempts / SUM(total_attempts) OVER (), 2) as percentage_of_total_volume
FROM daily_stats
ORDER BY day_of_month; 