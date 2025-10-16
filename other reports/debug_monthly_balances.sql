-- Debug Monthly Loan Balances Query
-- This will help identify where the data is being filtered out

-- Test 1: Check if month_spine is generating dates
SELECT 'month_spine' as test_step, COUNT(*) as record_count, 
       MIN(month_end) as min_date, MAX(month_end) as max_date
FROM (
    SELECT 
        DATE_TRUNC('month', DATEADD('month', seq4(), MIN(disbursementdate))) AS month_start,
        LAST_DAY(DATEADD('month', seq4(), MIN(disbursementdate))) AS month_end
    FROM UG_PROD.ML.LOAN_INFO_TBL
    WHERE disbursementdate IS NOT NULL
    HAVING month_end <= CURRENT_DATE
);

-- Test 2: Check if loan_info has data
SELECT 'loan_info' as test_step, COUNT(*) as record_count,
       MIN(disbursementdate) as min_disbursement, MAX(disbursementdate) as max_disbursement
FROM (
    SELECT 
        m.loan_id,
        m.loan_key,
        m.client_id,
        m.disbursementdate,
        m.loanamount as disbursed_amount,
        lp.id AS product_id,
        la.interestrate as daily_rate,
        la.encodedkey as loan_account_key
    FROM UG_PROD.ML.LOAN_INFO_TBL m
    JOIN UG_PROD.MAMBU.loanaccount la ON la.encodedkey = m.loan_key
    JOIN UG_PROD.MAMBU.LOANPRODUCT lp ON lp.ENCODEDKEY = m.PRODUCTTYPEKEY
    WHERE m.disbursementdate IS NOT NULL
);

-- Test 3: Check if we can get transaction data for a specific loan
SELECT 'sample_transactions' as test_step, COUNT(*) as record_count
FROM UG_PROD.MAMBU.loantransaction
WHERE parentaccountkey IN (
    SELECT m.loan_key 
    FROM UG_PROD.ML.LOAN_INFO_TBL m 
    WHERE m.disbursementdate IS NOT NULL 
    LIMIT 1
);

-- Test 4: Check if we can get the latest transaction for a loan
SELECT 'latest_transaction' as test_step, COUNT(*) as record_count
FROM (
    SELECT 
        parentaccountkey, 
        principalbalance, 
        balance, 
        entrydate,
        transactionid
    FROM UG_PROD.MAMBU.loantransaction
    WHERE parentaccountkey IN (
        SELECT m.loan_key 
        FROM UG_PROD.ML.LOAN_INFO_TBL m 
        WHERE m.disbursementdate IS NOT NULL 
        LIMIT 1
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY parentaccountkey ORDER BY transactionid DESC) = 1
);

-- Test 5: Check if the month filtering is working
SELECT 'month_filter_test' as test_step, COUNT(*) as record_count
FROM (
    SELECT 
        ms.month_end as report_month,
        li.loan_id,
        li.loan_key,
        li.disbursed_amount
    FROM (
        SELECT 
            DATE_TRUNC('month', DATEADD('month', seq4(), MIN(disbursementdate))) AS month_start,
            LAST_DAY(DATEADD('month', seq4(), MIN(disbursementdate))) AS month_end
        FROM UG_PROD.ML.LOAN_INFO_TBL
        WHERE disbursementdate IS NOT NULL
        HAVING month_end <= CURRENT_DATE
    ) ms
    CROSS JOIN (
        SELECT 
            m.loan_id,
            m.loan_key,
            m.disbursementdate,
            m.loanamount as disbursed_amount
        FROM UG_PROD.ML.LOAN_INFO_TBL m
        WHERE m.disbursementdate IS NOT NULL
        LIMIT 5  -- Limit to first 5 loans for testing
    ) li
    WHERE ms.month_end >= DATE_TRUNC('month', li.disbursementdate)
);

-- Test 6: Check if balance filtering is too restrictive
SELECT 'balance_filter_test' as test_step, COUNT(*) as record_count,
       SUM(CASE WHEN balance > 0 THEN 1 ELSE 0 END) as positive_balance_count,
       SUM(CASE WHEN balance = 0 THEN 1 ELSE 0 END) as zero_balance_count,
       SUM(CASE WHEN balance IS NULL THEN 1 ELSE 0 END) as null_balance_count
FROM (
    SELECT 
        parentaccountkey,
        balance
    FROM UG_PROD.MAMBU.loantransaction
    WHERE parentaccountkey IN (
        SELECT m.loan_key 
        FROM UG_PROD.ML.LOAN_INFO_TBL m 
        WHERE m.disbursementdate IS NOT NULL 
        LIMIT 10
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY parentaccountkey ORDER BY transactionid DESC) = 1
);

-- Test 7: Check if the date filtering is too restrictive
SELECT 'date_filter_test' as test_step, COUNT(*) as record_count
FROM (
    SELECT 
        ms.month_end as report_month,
        li.loan_id,
        li.loan_key,
        lt.entrydate,
        lt.principalbalance,
        lt.balance
    FROM (
        SELECT 
            DATE_TRUNC('month', DATEADD('month', seq4(), MIN(disbursementdate))) AS month_start,
            LAST_DAY(DATEADD('month', seq4(), MIN(disbursementdate))) AS month_end
        FROM UG_PROD.ML.LOAN_INFO_TBL
        WHERE disbursementdate IS NOT NULL
        HAVING month_end <= CURRENT_DATE
    ) ms
    CROSS JOIN (
        SELECT 
            m.loan_id,
            m.loan_key,
            m.disbursementdate,
            m.loanamount as disbursed_amount
        FROM UG_PROD.ML.LOAN_INFO_TBL m
        WHERE m.disbursementdate IS NOT NULL
        LIMIT 5  -- Limit to first 5 loans for testing
    ) li
    LEFT JOIN (
        SELECT 
            parentaccountkey, 
            principalbalance, 
            balance, 
            entrydate
        FROM UG_PROD.MAMBU.loantransaction
        WHERE parentaccountkey = li.loan_key
        QUALIFY ROW_NUMBER() OVER (PARTITION BY parentaccountkey ORDER BY transactionid DESC) = 1
    ) lt ON lt.parentaccountkey = li.loan_key
    WHERE ms.month_end >= DATE_TRUNC('month', li.disbursementdate)
      AND lt.principalbalance IS NOT NULL
      AND COALESCE(lt.balance, 0) > 0
);
