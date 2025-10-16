WITH may_2025_loans AS (
    SELECT 
        li.loan_id,
        li.client_id,
        li.loanamount,
        li.disbursementdate,
        li.LAST_EXPECTED_REPAYMENT,
        li.LN,
        CASE 
            WHEN li.loan_product_id LIKE '%UCBLL%' THEN 'FidoBiz'
            ELSE 'Personal'
        END as loan_type,
        rte.installment,
        rte.repayment_due_date,
        rte.total_due,
        rte.amount as paid_amount,
        rte.transaction_date,
        rte.repayment_state
    FROM ml.loan_info_tbl li
    LEFT JOIN ml.repayment_transactions_extended rte 
        ON li.loan_key = rte.loan_key
    WHERE DATE_TRUNC('month', li.disbursementdate) = '2025-05-01'  -- Loans disbursed in May 2025
    AND li.disbursementdate IS NOT NULL
),

outstanding_installments AS (
    SELECT 
        loan_id,
        client_id,
        loanamount,
        disbursementdate,
        LAST_EXPECTED_REPAYMENT,
        LN,
        loan_type,
        installment,
        repayment_due_date,
        total_due,
        COALESCE(SUM(CASE WHEN repayment_state IN ('PAID', 'PARTIALLY_PAID') THEN paid_amount ELSE 0 END), 0) as total_paid,
        (total_due - COALESCE(SUM(CASE WHEN repayment_state IN ('PAID', 'PARTIALLY_PAID') THEN paid_amount ELSE 0 END), 0)) as outstanding_amount,
        CASE 
            WHEN (total_due - COALESCE(SUM(CASE WHEN repayment_state IN ('PAID', 'PARTIALLY_PAID') THEN paid_amount ELSE 0 END), 0)) > 0 THEN 'Outstanding'
            ELSE 'Paid'
        END as status,
        DATEDIFF('day', repayment_due_date, CURRENT_DATE) as days_overdue
    FROM may_2025_loans
    GROUP BY 
        loan_id, client_id, loanamount, disbursementdate, LAST_EXPECTED_REPAYMENT, 
        LN, loan_type, installment, repayment_due_date, total_due
)

SELECT 
    loan_id,
    client_id,
    loanamount,
    disbursementdate,
    LAST_EXPECTED_REPAYMENT,
    LN,
    loan_type,
    installment,
    repayment_due_date,
    total_due,
    total_paid,
    outstanding_amount,
    status,
    days_overdue
FROM outstanding_installments
WHERE status = 'Outstanding'
AND outstanding_amount > 0
ORDER BY days_overdue DESC, outstanding_amount DESC; 