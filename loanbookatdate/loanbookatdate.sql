WITH components_due_amount AS (
	SELECT PARENTACCOUNTKEY,
	       SUM(PRINCIPAL_DUE) AS PRINCIPAL_DUE, 
	       COALESCE(SUM(FEES_DUE),0) AS FEES_DUE, 
	       SUM(INTEREST_DUE) AS INTEREST_DUE, 
	       COALESCE(SUM(TOTAL_INTEREST_ACCRUED),0) AS INTEREST_ACCRUED,
	       SUM(PENALTY_DUE) AS PENALTY_DUE,
	       COALESCE(SUM(PENALTY_ACCRUED),0) AS PENALTY_ACCRUED
	FROM (
		    SELECT PARENTACCOUNTKEY, REPAYMENTSCHEDULEMETHOD, DUEDATE, r.STATE,
			       r.PRINCIPALDUE AS PRINCIPAL_EXPECTED, r.PRINCIPALPAID AS PRINCIPAL_PAID, PRINCIPAL_EXPECTED - PRINCIPAL_PAID AS PRINCIPAL_DUE,      
			       CASE WHEN DATE(DUEDATE) > CURRENT_DATE AND STATE != 'PAID' AND REPAYMENTSCHEDULEMETHOD = 'DYNAMIC' THEN DUEDATE ELSE NULL END AS OPEN_DYNAMIC_INSTALLMENT_DUE_DATE,
	               CASE WHEN OPEN_DYNAMIC_INSTALLMENT_DUE_DATE IS NOT NULL THEN CASE WHEN ROW_NUMBER() OVER (PARTITION BY PARENTACCOUNTKEY ORDER BY OPEN_DYNAMIC_INSTALLMENT_DUE_DATE) = 1 THEN LA.ACCRUEDINTEREST - LA.INTERESTFROMARREARSACCRUED END END AS TOTAL_INTEREST_ACCRUED,
			       CASE WHEN DATE(DUEDATE) <= CURRENT_DATE OR REPAYMENTSCHEDULEMETHOD = 'FIXED' THEN R.INTERESTDUE ELSE 0 END AS INTEREST_EXPECTED, 
			       r.INTERESTPAID AS INTEREST_PAID, 
			       CASE WHEN DATE(duedate) <= CURRENT_DATE OR REPAYMENTSCHEDULEMETHOD = 'FIXED' THEN INTEREST_EXPECTED - INTEREST_PAID ELSE 0 END AS INTEREST_DUE,
			       r.FEESDUE AS FEES_EXPECTED, r.FEESPAID AS FEES_PAID, FEES_EXPECTED - FEES_PAID AS FEES_DUE,
			       r.PENALTYDUE AS PENALTY_EXPECTED, r.PENALTYPAID AS PENALTY_PAID, PENALTY_EXPECTED - PENALTY_PAID AS PENALTY_DUE,
			       CASE WHEN ROW_NUMBER() OVER (PARTITION BY PARENTACCOUNTKEY ORDER BY OPEN_DYNAMIC_INSTALLMENT_DUE_DATE) = 1 AND (LA.LASTLOCKEDDATE IS NULL OR DATE(LA.LASTLOCKEDDATE) > CURRENT_DATE)
			       THEN LA.ACCRUEDPENALTY ELSE 0.0 END AS PENALTY_ACCRUED 
			FROM UG_PROD.MAMBU.REPAYMENT r
			JOIN UG_PROD.MAMBU.LOANACCOUNT la ON r.PARENTACCOUNTKEY = la.ENCODEDKEY 
	) 
	GROUP BY PARENTACCOUNTKEY
),
-- Generate all month-ends from earliest loan to current date
month_spine AS (
    SELECT 
        DATE_TRUNC('month', DATEADD('month', seq4(), MIN(disbursementdate))) AS month_start,
        LAST_DAY(DATEADD('month', seq4(), MIN(disbursementdate))) AS month_end
    FROM UG_PROD.ML.LOAN_INFO_TBL
    WHERE disbursementdate IS NOT NULL
    HAVING month_end <= CURRENT_DATE
),
-- Get loan static information
loan_static_info AS (
    SELECT 
        m.loan_id,
        m.loan_key,
        m.client_key,
        m.client_id,
        c.client_id AS client_mambu_id,
        m.ln,
        m.disbursementdate,
        lp.id AS product_id,
        m.loanamount as disbursed_amount,
        c.CLIENT_FIRST_DISBURSEMENT_DATE,
        c.industry,
        c.gender,
        m.USE_OF_FUNDS_TYPE as use_of_funds,
        c.employment,
        la.interestrate as daily_rate,
        -- Product group classification
        CASE WHEN m.LOAN_PRODUCT_ID LIKE '%UCLL%' THEN 'Personal' 
             WHEN (m.LOAN_PRODUCT_ID LIKE '%UCBLL%' AND ml.LN <= 3) OR ml.DISBURSEMENTDATE IS NULL THEN 'New_FidoBiz'
             WHEN m.LOAN_PRODUCT_ID LIKE '%UCBLL%' AND ml.LN > 3 THEN 'Migrated_FidoBiz' 
        END as product_group_detailed,
        CASE WHEN m.LOAN_PRODUCT_ID LIKE 'UCBLL%' THEN 'FidoBiz' 
             WHEN m.LOAN_PRODUCT_ID LIKE '%UG_UCBLL%' THEN 'POC' 
             ELSE 'General' 
        END as product_group,
        CASE WHEN m.ln > 0 THEN 'Y' ELSE 'N' END as repeat_client
    FROM UG_PROD.ML.LOAN_INFO_TBL m
    LEFT JOIN UG_PROD.TABLEAU.CLIENTS c ON m.client_key = c.client_key
    JOIN UG_PROD.MAMBU.loanaccount la ON la.encodedkey = m.loan_key
    JOIN UG_PROD.MAMBU.LOANPRODUCT lp ON lp.ENCODEDKEY = m.PRODUCTTYPEKEY
    LEFT JOIN (
        SELECT client_id, disbursementdate, LN 
        FROM UG_PROD.ml.LOAN_INFO_TBL ml 
        WHERE ml.disbursementdate IS NOT NULL AND ml.LOAN_PRODUCT_ID NOT LIKE '%UCBLL%'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY CLIENT_ID ORDER BY DISBURSEMENTDATE DESC) = 1
    ) ml ON ml.client_id = m.client_id
    WHERE m.disbursementdate IS NOT NULL
),
-- Calculate loan state at end of each month
loan_monthly_state AS (
    SELECT 
        ms.month_end as report_month,
        lsi.loan_id,
        lsi.loan_key,
        lsi.client_key,
        lsi.client_id,
        lsi.client_mambu_id,
        lsi.ln,
        lsi.disbursementdate,
        lsi.product_id,
        lsi.disbursed_amount,
        lsi.CLIENT_FIRST_DISBURSEMENT_DATE,
        lsi.industry,
        lsi.gender,
        lsi.use_of_funds,
        lsi.employment,
        lsi.daily_rate,
        lsi.product_group_detailed,
        lsi.product_group,
        lsi.repeat_client,
        -- Get the latest transaction state as of month end
        l.principalbalance,
        l.balance as total_balance,
        CASE WHEN l.principalbalance < lsi.disbursed_amount THEN l.principalbalance ELSE lsi.disbursed_amount END as principal_balance,
        -- Get due amounts
        cda.PRINCIPAL_DUE,
        cda.FEES_DUE,
        cda.INTEREST_DUE,
        cda.INTEREST_ACCRUED,
        cda.PENALTY_DUE,
        cda.PENALTY_ACCRUED,
        -- Calculate total repayments up to this month
        COALESCE(SUM(CASE WHEN lt."type" IN ('REPAYMENT', 'REPAYMENT_ADJUSTMENT') AND DATE(lt.entrydate) <= ms.month_end 
                     THEN lt.amount ELSE 0 END), 0) as total_repayments,
        -- Get processing fee
        pf.amount as processing_fee,
        -- Get loan status
        CASE WHEN m.ACCOUNTSUBSTATE = 'LOCKED' THEN TRUE ELSE FALSE END AS is_loan_locked,
        -- Get latest due date
        r.duedate as due_date,
        -- Calculate days past due
        DATEDIFF('day', r.duedate, ms.month_end) as days_past_last_due_date,
        -- Add creation date for filtering logic
        DATE_TRUNC('day', l.entrydate) as creation_date
    FROM month_spine ms
    CROSS JOIN loan_static_info lsi
    -- Get the most recent transaction state as of month end
    LEFT JOIN (
        SELECT parentaccountkey, principalbalance, balance, entrydate, transactionid
        FROM UG_PROD.MAMBU.loantransaction lt
        WHERE lt.parentaccountkey = lsi.loan_key
          AND DATE(lt.entrydate) <= ms.month_end
        QUALIFY ROW_NUMBER() OVER (PARTITION BY parentaccountkey ORDER BY transactionid DESC) = 1
    ) l ON l.parentaccountkey = lsi.loan_key
    -- Get components due amount
    LEFT JOIN components_due_amount cda ON cda.PARENTACCOUNTKEY = lsi.loan_key
    -- Get processing fee
    LEFT JOIN UG_PROD.MAMBU.PredefinedFee pf ON pf.loanfees_encodedkey_own = lsi.loan_key AND pf.name = 'Processing Fee'
    -- Get loan account for status
    LEFT JOIN UG_PROD.MAMBU.LOANACCOUNT m ON m.encodedkey = lsi.loan_key
    -- Get repayment schedule for due date (latest due date as of month end)
    LEFT JOIN (
        SELECT PARENTACCOUNTKEY, duedate
        FROM UG_PROD.MAMBU.REPAYMENT r
        WHERE r.PARENTACCOUNTKEY = lsi.loan_key
          AND DATE(r.duedate) <= ms.month_end
        QUALIFY ROW_NUMBER() OVER (PARTITION BY PARENTACCOUNTKEY ORDER BY duedate DESC) = 1
    ) r ON r.PARENTACCOUNTKEY = lsi.loan_key
    -- Get all transactions for repayment calculation
    LEFT JOIN UG_PROD.MAMBU.loantransaction lt ON lt.parentaccountkey = lsi.loan_key
    WHERE ms.month_end >= DATE_TRUNC('month', lsi.disbursementdate)  -- Only include months after disbursement
      AND l.principalbalance IS NOT NULL  -- Loan must have existed in this month
      AND DATE_TRUNC('day', l.entrydate) <= ms.month_end  -- Transaction must have occurred by month end
    GROUP BY ms.month_end, lsi.loan_id, lsi.loan_key, lsi.client_key, lsi.client_id, lsi.client_mambu_id, lsi.ln, lsi.disbursementdate, lsi.product_id, lsi.disbursed_amount, lsi.CLIENT_FIRST_DISBURSEMENT_DATE, lsi.industry, lsi.gender, lsi.use_of_funds, lsi.employment, lsi.daily_rate, lsi.product_group_detailed, lsi.product_group, lsi.repeat_client, l.principalbalance, l.balance, cda.PRINCIPAL_DUE, cda.FEES_DUE, cda.INTEREST_DUE, cda.INTEREST_ACCRUED, cda.PENALTY_DUE, cda.PENALTY_ACCRUED, pf.amount, m.ACCOUNTSUBSTATE, r.duedate, l.entrydate
),
-- Calculate PAR using simplified logic
par_calculations AS (
    SELECT 
        ms.month_end as monthb, 
        rt.loan_id,
        MIN(CASE WHEN rt.TOTAL_REPAYMENT_AMOUNT < rt.TOTAL_DUE THEN rt.REPAYMENT_DUE_DATE END) as earliest_open_installment_duedate,
        CASE 
            WHEN MIN(CASE WHEN rt.TOTAL_REPAYMENT_AMOUNT < rt.TOTAL_DUE THEN rt.REPAYMENT_DUE_DATE END) IS NOT NULL 
            THEN DATEDIFF('day', MIN(CASE WHEN rt.TOTAL_REPAYMENT_AMOUNT < rt.TOTAL_DUE THEN rt.REPAYMENT_DUE_DATE END), LEAST(ms.month_end, CURRENT_DATE))
            ELSE NULL 
        END as par
    FROM month_spine ms
    CROSS JOIN ML.REPAYMENT_TRANSACTIONS_EXTENDED rt
    LEFT JOIN ml.LOAN_INFO_TBL l ON l.LOAN_ID = rt.LOAN_ID
    LEFT JOIN (
        SELECT client_id, disbursementdate, LN 
        FROM UG_PROD.ml.LOAN_INFO_TBL ml 
        WHERE ml.disbursementdate IS NOT NULL AND ml.LOAN_PRODUCT_ID NOT LIKE '%UCBLL%'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY CLIENT_ID ORDER BY DISBURSEMENTDATE DESC) = 1
    ) ml ON ml.client_id = l.client_id 
    WHERE CASE WHEN l.LOAN_PRODUCT_ID LIKE '%UCLL%' THEN 'Personal' 
               WHEN (l.LOAN_PRODUCT_ID LIKE '%UCBLL%' AND ml.LN <= 3) OR ml.DISBURSEMENTDATE IS NULL THEN 'New_FidoBiz'
               WHEN (l.LOAN_PRODUCT_ID LIKE '%UCBLL%' AND ml.LN > 3) THEN 'Migrated_FidoBiz' 
          END IN ('Personal', 'New_FidoBiz', 'Migrated_FidoBiz')
      AND rt.TOTAL_REPAYMENT_AMOUNT < rt.TOTAL_DUE  -- Only loans with outstanding balance
      AND LAST_DAY(rt.TRANSACTION_DATE) <= ms.month_end  -- Transactions up to month end
    GROUP BY 1, 2
)
-- Final result with PAR classification
SELECT 
    lms.report_month,
    TO_CHAR(lms.disbursementdate, 'YYYY-MM') as month_disbursed,
    lms.loan_id,
    lms.client_mambu_id,
    lms.repeat_client,
    lms.product_group,
    lms.product_group_detailed,
    lms.product_id,
    lms.disbursementdate,
    lms.disbursed_amount,
    lms.processing_fee,
    lms.daily_rate * 365/100 as interest_rate_annual,
    lms.ln,
    lms.due_date,
    lms.total_repayments as repayments,
    lms.CLIENT_FIRST_DISBURSEMENT_DATE,
    lms.industry,
    lms.gender,
    lms.use_of_funds,
    lms.employment,
    lms.is_loan_locked,
    lms.principal_balance,
    lms.total_balance as total_loan_balance,
    lms.PRINCIPAL_DUE,
    lms.FEES_DUE,
    lms.INTEREST_DUE,
    lms.INTEREST_ACCRUED,
    lms.PENALTY_DUE,
    lms.PENALTY_ACCRUED,
    lms.creation_date,
    -- Calculate final PAR
    CASE WHEN pc.par IS NOT NULL THEN pc.par ELSE lms.days_past_last_due_date END as days_past_due_date,
    CASE WHEN days_past_due_date <= 2 THEN 'Performing'
         WHEN days_past_due_date BETWEEN 3 AND 32 THEN 'Watch'
         WHEN days_past_due_date BETWEEN 33 AND 92 THEN 'Substandard'
         WHEN days_past_due_date BETWEEN 93 AND 182 THEN 'Doubtful'
         WHEN days_past_due_date > 182 THEN 'Loss' 
    END as par_classification
FROM loan_monthly_state lms
LEFT JOIN par_calculations pc ON lms.loan_id = pc.loan_id AND lms.report_month = pc.monthb
-- Only include records where loan had a balance > 0 at month end
WHERE lms.principal_balance > 0
  AND lms.creation_date <= lms.report_month  -- Ensure loan existed at month end
  AND report_month = '2025-07-30'
ORDER BY lms.loan_id, lms.report_month;