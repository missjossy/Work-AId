WITH payment_transactions AS (
    SELECT 
        rte.loan_key,
        li.loan_id,
        rte.client_id,
        li.disbursementdate,
        rte.installment,
        rte.repayment_due_date,
        rte.transaction_date,
        rte.amount as payment_amount,
        rte.total_due,
        li.penaltypaid,
        CASE 
            WHEN li.LN = 0 THEN 'LN0'
            WHEN li.LN = 1 THEN 'LN1'
            WHEN li.LN = 2 THEN 'LN2'
            WHEN li.LN BETWEEN 3 AND 5 THEN 'LN3-5'
            WHEN li.LN BETWEEN 6 AND 8 THEN 'LN6-8'
            WHEN li.LN BETWEEN 9 AND 11 THEN 'LN9-11'
            ELSE 'LN12+'
        END as ln_group,
        CASE 
            WHEN li.repaymentinstallments = 1 THEN 'regular'
            ELSE 'installments'
        END as loan_type,
        DATEDIFF('day', rte.repayment_due_date, rte.transaction_date) as days_late,
        DATE_TRUNC('month', rte.transaction_date) as payment_month
    FROM ml.repayment_transactions_extended rte
    JOIN ml.loan_info_tbl li ON rte.loan_key = li.loan_key
    WHERE 
        rte.disbursementdate IS NOT NULL
        AND YEAR(rte.disbursementdate) >= 2024
        AND rte.transaction_date > rte.repayment_due_date
        AND rte.amount > 0  -- Only count actual payments
        AND rte.status = 'succeeded'  -- Only count successful payments
        AND DATEDIFF('day', rte.repayment_due_date, rte.transaction_date) > 2  -- Only count payments in arrears period (beyond tolerance)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY rte.loan_key, rte.installment ORDER BY rte.transaction_date DESC) = 1  -- Take only the latest payment per installment
),

monthly_activity AS (
    SELECT 
        DATE_TRUNC('{{scale}}', disbursementdate) as period,
        ln_group,
        COUNT(DISTINCT client_id) as number_of_clients,
        COUNT(DISTINCT loan_id) as number_of_loans,
        COUNT(DISTINCT CASE WHEN penaltypaid > 0 THEN client_id END) as clients_with_penalties,
        COUNT(DISTINCT CASE WHEN penaltypaid > 0 THEN loan_id END) as loans_with_penalties
    FROM payment_transactions
    WHERE date(disbursementdate) BETWEEN date('{{ Range.start }}') and date('{{ Range.end }}')
    AND loan_type = '{{loan_type}}'
    GROUP BY 1, 2
)

SELECT * FROM monthly_activity
ORDER BY period, ln_group; 