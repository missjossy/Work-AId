-- Optimized version with simplified logic and better performance
WITH base_clients AS (
    SELECT DISTINCT
        cl.id as client_id
    FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa
    LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
    WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
    AND (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
    AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE2 is null) 
    AND date(sa.creationdate) >= date('2025-04-03')
    AND date(sa.creationdate) between '2025-04-03' and '2025-10-15'
),

-- Get withdrawals with client info
withdrawals AS (
    SELECT 
        cl.id as client_id,
        st.creationdate as withdrawal_date,
        st.amount as withdrawal_amount
    FROM GHANA_PROD.MAMBU.SAVINGSTRANSACTION st
    JOIN GHANA_PROD.MAMBU.SAVINGSACCOUNT sa ON st.parentaccountkey = sa.encodedkey
    JOIN MAMBU.CLIENT cl ON cl.encodedkey = sa.accountholderkey
    WHERE st."type" = 'WITHDRAWAL'
    AND date(st.creationdate) BETWEEN date('2025-04-03') and date('2025-10-15')
    AND (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
    AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE2 is null)
),

-- Get loan disbursements
loans AS (
    SELECT 
        client_id,
        disbursementdate,
        loanamount as loan_amount
    FROM ml.loan_info_tbl
    WHERE disbursementdate IS NOT NULL
    AND ACCOUNTSTATE != 'CLOSED_WRITTEN_OFF'
    AND date(disbursementdate) BETWEEN date('2025-04-03') and date('2025-10-15')
),

-- Get loan repayments
repayments AS (
    SELECT 
        li.client_id,
        rte.transaction_date as repayment_date,
        rte.amount as repayment_amount
    FROM ml.repayment_transactions_extended rte
    JOIN ml.loan_info_tbl li ON rte.loan_id = li.loan_id
    WHERE date(rte.transaction_date) BETWEEN date('2025-04-03') and date('2025-10-15')
),

-- Quick analysis: withdrawals before loans (within 7 days)
withdrawals_before_loans AS (
    SELECT 
        w.client_id,
        COUNT(*) as quick_loans_count,
        SUM(w.withdrawal_amount) as total_withdrawal_amount,
        SUM(l.loan_amount) as total_loan_amount
    FROM withdrawals w
    JOIN loans l ON w.client_id = l.client_id
    WHERE w.withdrawal_date <= l.disbursementdate
    AND DATEDIFF('day', w.withdrawal_date, l.disbursementdate) <= 7
    GROUP BY w.client_id
),

-- Quick analysis: withdrawals before repayments (within 3 days)
withdrawals_before_repayments AS (
    SELECT 
        w.client_id,
        COUNT(*) as quick_repayments_count,
        SUM(w.withdrawal_amount) as total_withdrawal_amount,
        SUM(r.repayment_amount) as total_repayment_amount
    FROM withdrawals w
    JOIN repayments r ON w.client_id = r.client_id
    WHERE w.withdrawal_date <= r.repayment_date
    AND DATEDIFF('day', w.withdrawal_date, r.repayment_date) <= 3
    GROUP BY w.client_id
)

-- Summary results
SELECT 
    'Quick Analysis' as analysis_type,
    COUNT(DISTINCT bc.client_id) as total_clients,
    COUNT(DISTINCT w.client_id) as clients_with_withdrawals,
    COUNT(DISTINCT l.client_id) as clients_with_loans,
    COUNT(DISTINCT wbl.client_id) as clients_withdrawing_before_loans,
    COUNT(DISTINCT wbr.client_id) as clients_withdrawing_before_repayments,
    SUM(w.withdrawal_amount) as total_withdrawals,
    SUM(l.loan_amount) as total_loans,
    SUM(wbl.total_withdrawal_amount) as withdrawals_before_loans_amount,
    SUM(wbl.total_loan_amount) as loans_after_withdrawals_amount,
    SUM(wbr.total_withdrawal_amount) as withdrawals_before_repayments_amount,
    SUM(wbr.total_repayment_amount) as repayments_after_withdrawals_amount
FROM base_clients bc
LEFT JOIN withdrawals w ON bc.client_id = w.client_id
LEFT JOIN loans l ON bc.client_id = l.client_id
LEFT JOIN withdrawals_before_loans wbl ON bc.client_id = wbl.client_id
LEFT JOIN withdrawals_before_repayments wbr ON bc.client_id = wbr.client_id;
