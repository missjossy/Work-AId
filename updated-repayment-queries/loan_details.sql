SELECT 
    m.loan_id AS account_number,
    m.client_key AS customer_id,
    'GHS' AS currency,
    m.loanamount AS loan_amount_approved,
    m.loanamount AS loan_amount_disbursed,
    date(m.APPROVEDDATE) AS date_of_approval,
    date(m.disbursementdate) AS loan_disbursement_date,
    date(m.LAST_EXPECTED_REPAYMENT) AS loan_maturity_date,
    'DYNAMIC_TERM_LOAN' AS loan_product,
    'Loan' AS loan_category,
    'Monthly' AS frequency_of_payments,
    la.interestrate AS interest_rate,
    'Fixed' AS interest_charge_type,
    m.REPAYMENTINSTALLMENTS AS number_of_payments_agreed,
    date(re.first_due_date) AS agreed_date_of_first_principal_repayment,
    CASE WHEN l.principalbalance < m.loanamount THEN l.principalbalance ELSE m.loanamount END AS loan_principal_balance,
    m.principalpaid AS amount_of_last_actual_principal_repayment,
    re.loan_interest_expected AS interest_receivable,
    CASE WHEN l."type" in ('REPAYMENT', 'REPAYMENT_ADJUSTMENT') THEN l.INTERESTAMOUNT ELSE 0 END AS interest_received
FROM GHANA_PROD.ML.LOAN_INFO_TBL m 
JOIN GHANA_PROD.MAMBU.loanaccount la ON la.encodedkey = m.loan_key
JOIN GHANA_PROD.MAMBU.loantransaction l ON m.loan_key = l.parentaccountkey
LEFT JOIN (
    SELECT PARENTACCOUNTKEY, 
           min(DUEDATE) AS first_due_date, 
           sum(INTERESTDUE) AS loan_interest_expected 
    FROM GHANA_PROD.MAMBU.REPAYMENT 
    GROUP BY PARENTACCOUNTKEY
) re ON re.PARENTACCOUNTKEY = m.LOAN_KEY
WHERE l.creationdate <= date_trunc('day',date('2025-04-30'))
QUALIFY ROW_NUMBER() OVER (PARTITION BY m.loan_id ORDER BY l.transactionid DESC) = 1; 