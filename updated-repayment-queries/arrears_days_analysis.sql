WITH base_loans AS (
    SELECT 
        li.loan_key,
        li.loan_id,
        CASE 
            WHEN li.LN = 0 THEN 'LN0'
            WHEN li.LN = 1 THEN 'LN1'
            WHEN li.LN = 2 THEN 'LN2'
            WHEN li.LN BETWEEN 3 AND 5 THEN 'LN3-5'
            WHEN li.LN BETWEEN 6 AND 8 THEN 'LN6-8'
            WHEN li.LN BETWEEN 9 AND 11 THEN 'LN9-11'
            ELSE 'LN12+'
        END as ln_group,
        rte.repayment_due_date,
        rte.transaction_date,
        DATEDIFF('day', rte.repayment_due_date, rte.transaction_date) as days_in_arrears
    FROM ml.loan_info_tbl li
    JOIN ml.repayment_transactions_extended rte 
        ON li.loan_key = rte.loan_key
        AND rte.transaction_date > rte.repayment_due_date
        AND DATEDIFF('day', rte.repayment_due_date, rte.transaction_date) > 2  -- Only arrears period payments
        AND rte.amount >= rte.total_due  -- Only fully paid installments
    WHERE li.disbursementdate IS NOT NULL
        AND li.disbursementdate >= '2024-01-01'
        AND days_from_first_due_installment >= 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY li.loan_key, rte.installment ORDER BY rte.transaction_date DESC) = 1
)

SELECT 
    ln_group,
    days_in_arrears,
    COUNT(*) as number_of_payments
FROM base_loans
WHERE disbursementdate BETWEEN date('{{Range.start}}') AND date('{{Range.end}}')
GROUP BY 
    ln_group,
    days_in_arrears
ORDER BY 
    CASE ln_group
        WHEN 'LN0' THEN 0
        WHEN 'LN1' THEN 1
        WHEN 'LN2' THEN 2
        WHEN 'LN3-5' THEN 3
        WHEN 'LN6-8' THEN 6
        WHEN 'LN9-11' THEN 9
        ELSE 12
    END,
    days_in_arrears; 