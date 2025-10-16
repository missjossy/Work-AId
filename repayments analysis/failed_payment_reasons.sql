WITH relevant_notifications AS (
    SELECT 
        timestamp,
        "TYPE",
        PARSE_JSON(PAYLOAD):user_id::string as user_id,
        PARSE_JSON(PAYLOAD):client_id::string as client_id,
        PARSE_JSON(PAYLOAD):status::string as status,
        PARSE_JSON(PAYLOAD):error_reason::string as error_reason,
        PARSE_JSON(PAYLOAD):error_code::string as error_code
    FROM data.backend_notifications
    WHERE ("TYPE" = 'BE_AUTHENTICATION_PHONE_VERIFICATION_2' OR "TYPE" = 'BE_LOAN_REPAYMENT_RESULT')
    AND TO_TIMESTAMP_NTZ(timestamp) >= '2024-01-01'
),

failed_payment_details AS (
    SELECT 
        error_reason as failure_reason,
        error_code,
        COUNT(*) as failure_count
    FROM relevant_notifications
    WHERE "TYPE" = 'BE_LOAN_REPAYMENT_RESULT'
    AND status != 'succeeded'
    GROUP BY 1, 2
)

SELECT 
    COALESCE(failure_reason, 'Unknown') as failure_reason,
    error_code,
    failure_count,
    ROUND(100.0 * failure_count / SUM(failure_count) OVER (), 2) as percentage_of_failures
FROM failed_payment_details
ORDER BY failure_count DESC; 