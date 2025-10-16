WITH par_calculation AS (
    SELECT 
        loan_id,
        monthb,
        par
    FROM (
        SELECT 
            monthb,
            loan_id,
            min(CASE WHEN TOTAL_REPAYMENT_AMOUNTT::float < TOTALDUE::float 
                THEN DATEDIFF('day', REPAYMENT_DUE_DATE, least(monthb,CURRENT_DATE)) 
            END) AS par
        FROM (
            SELECT 
                MONTH AS monthb,
                rt.loan_id,
                REPAYMENT_DUE_DATE,
                max(TOTAL_DUE) AS TOTALDUE,
                nvl(sum(CASE WHEN last_day(rt.TRANSACTION_DATE) <= monthb 
                    THEN rt.amount ELSE 0 END), 0) AS TOTAL_REPAYMENT_AMOUNTT
            FROM ML.REPAYMENT_TRANSACTIONS_EXTENDED rt
            JOIN (
                SELECT parentaccountkey, MAX(last_day(creationdate)) MONTH
                FROM mambu.loantransaction
                WHERE creationdate <= '2025-04-30'
                GROUP BY 1
            ) tt ON rt.loan_key = tt.parentaccountkey
            LEFT JOIN ml.LOAN_INFO_TBL l ON l.LOAN_ID = rt.LOAN_ID
            WHERE CASE WHEN l.LOAN_PRODUCT_ID LIKE '%UCBLL%' 
                THEN 'FidoBiz' ELSE 'General' END IN ('General', 'FidoBiz')
            AND installment > 1
            GROUP BY 1, 2, 3
        )
        WHERE TOTALDUE > TOTAL_REPAYMENT_AMOUNTT
        GROUP BY 1, 2
    )
)

SELECT 
    m.loan_id AS account_number,
    CASE 
        WHEN par IS NULL THEN DATEDIFF('day', m.LAST_EXPECTED_REPAYMENT, CURRENT_DATE)
        ELSE par
    END AS days_in_arrears,
    CASE 
        WHEN days_in_arrears <= 2 OR days_in_arrears IS NULL THEN 'current' 
        WHEN days_in_arrears BETWEEN 3 AND 32 THEN 'par 0'
        WHEN days_in_arrears BETWEEN 33 AND 62 THEN 'par 30' 
        WHEN days_in_arrears BETWEEN 63 AND 92 THEN 'par 60'
        WHEN days_in_arrears BETWEEN 93 AND 122 THEN 'par 90'
        WHEN days_in_arrears BETWEEN 123 AND 152 THEN 'par 120'
        ELSE 'par 150' 
    END AS bog_loan_classification,
    CASE 
        WHEN days_in_arrears <= 2 OR days_in_arrears IS NULL THEN 0.01
        WHEN days_in_arrears BETWEEN 3 AND 32 THEN 0.01
        WHEN days_in_arrears BETWEEN 33 AND 62 THEN 0.2
        WHEN days_in_arrears BETWEEN 63 AND 92 THEN 0.4
        WHEN days_in_arrears BETWEEN 93 AND 122 THEN 0.6
        WHEN days_in_arrears BETWEEN 123 AND 152 THEN 0.8
        ELSE 1
    END AS bog_provision,
    CASE 
        WHEN days_in_arrears <= 60 OR days_in_arrears IS NULL THEN 'Stage 1' 
        WHEN days_in_arrears BETWEEN 61 AND 120 THEN 'Stage 2'
        ELSE 'Stage 3' 
    END AS ifrs_classification
FROM GHANA_PROD.ML.LOAN_INFO_TBL m
LEFT JOIN par_calculation p ON p.loan_id = m.loan_id; 