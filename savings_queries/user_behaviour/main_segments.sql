WITH base_savings_data AS (
    SELECT DISTINCT
        cl.id as client_id, 
        sa.id as account_id,
        sa.creationdate as creation_date,
        sa.closeddate,
        sa.accountstate,
        sa.encodedkey as account_key,
        cl.birthdate,
        DATEDIFF('year', cl.birthdate, current_date) as age
    FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa
    LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
    WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
    AND (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
    AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE2 is null) 
    AND sa.accountstate !='WITHDRAWN'
    AND date(sa.creationdate) >= date('2025-04-03')
    AND date(sa.creationdate) between '2025-04-03' and '2025-10-20'
),

account_gaps AS (
    SELECT 
        client_id,
        account_id,
        creation_date,
        closeddate,
        account_key,
        LAG(creation_date) OVER (PARTITION BY client_id ORDER BY creation_date) as previous_creation_date,
        DATEDIFF(day, LAG(creation_date) OVER (PARTITION BY client_id ORDER BY creation_date), creation_date) as days_since_previous_account,
        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY creation_date DESC) as account_rank
    FROM base_savings_data
),

multi_account_churned AS (
    SELECT 
        client_id,
        MAX(days_since_previous_account) as max_gap_days
    FROM account_gaps
    WHERE days_since_previous_account IS NOT NULL
    GROUP BY client_id
    HAVING MAX(days_since_previous_account) > 25
),

single_account_churned AS (
    SELECT 
        client_id,
        account_id,
        creation_date,
        closeddate,
        DATEDIFF(day, closeddate, '2025-10-20') as days_since_closed
    FROM base_savings_data
    WHERE closeddate IS NOT NULL
    AND DATEDIFF(day, closeddate, '2025-10-20') > 25
    AND client_id NOT IN (SELECT client_id FROM multi_account_churned)
),

all_churned_clients AS (
    SELECT client_id FROM multi_account_churned
    UNION
    SELECT client_id FROM single_account_churned
),

churn_status AS (
    SELECT 
        ag.client_id,
        CASE 
            WHEN ag.client_id IN (SELECT client_id FROM all_churned_clients) THEN TRUE
            ELSE FALSE
        END as is_churned,
        CASE 
            WHEN ag.closeddate IS NULL THEN TRUE  -- Most recent account still active
            WHEN DATEDIFF(day, ag.closeddate, '2025-10-20') < 25 THEN TRUE  -- Most recent account closed < 25 days ago
            ELSE FALSE  -- Most recent account closed >= 25 days ago
        END as is_returned,
        COALESCE(mac.max_gap_days, sac.days_since_closed) as churn_indicator_days,
        CASE 
            WHEN mac.max_gap_days IS NOT NULL THEN 'multi_account_gap'
            WHEN sac.days_since_closed IS NOT NULL THEN 'single_account_closure'
            ELSE 'not_churned'
        END as churn_type
    FROM account_gaps ag
    LEFT JOIN multi_account_churned mac ON ag.client_id = mac.client_id
    LEFT JOIN single_account_churned sac ON ag.client_id = sac.client_id
    WHERE ag.account_rank = 1
),

transaction_data AS (
    SELECT 
        st.parentaccountkey as account_key,
        st.creationdate as transaction_date,
        st.amount,
        st."type" as transaction_type,
        st.balance
    FROM GHANA_PROD.MAMBU.SAVINGSTRANSACTION st
    WHERE st."type" IN ('WITHDRAWAL', 'DEPOSIT')
    AND date(st.creationdate) BETWEEN date('2025-04-03') and date('2025-10-20')
),

-- Get loan history for each client
loan_history AS (
    SELECT 
        client_id,
        MAX(ln) as max_ln,
        MIN(disbursementdate) as first_loan_date,
        MAX(disbursementdate) as last_loan_date,
        COUNT(DISTINCT loan_id) as total_loans
    FROM ml.loan_info_tbl
    WHERE disbursementdate IS NOT NULL
    AND ACCOUNTSTATE != 'CLOSED_WRITTEN_OFF'
    GROUP BY client_id
),
loan_at_savings_creation AS (
    SELECT 
        bsd.client_id,
        li.loan_id,
        li.ln,
        li.disbursementdate,
        li.accountstate,
        bsd.first_creation_date as savings_creation_date,
        rh.last_repayment_date,
        -- Calculate days between loan closure and savings creation using actual last repayment date
        DATEDIFF('day', 
            CASE 
                WHEN li.accountstate IN ('CLOSED', 'CLOSED_WRITTEN_OFF', 'CLOSED_RESCHEDULED') 
                THEN rh.last_repayment_date  -- Use actual last repayment date
                ELSE NULL 
            END, 
            bsd.first_creation_date
        ) as days_since_loan_closure,
        -- Determine if loan was active at savings creation
        CASE 
            WHEN li.disbursementdate <= bsd.first_creation_date 
                 AND (li.accountstate = 'ACTIVE' OR li.accountstate = 'ACTIVE_IN_ARREARS')
            THEN 'active_at_creation'
            WHEN li.disbursementdate <= bsd.first_creation_date 
                 AND li.accountstate NOT IN ('ACTIVE', 'ACTIVE_IN_ARREARS')
                 AND DATEDIFF('day', rh.last_repayment_date, bsd.first_creation_date) > 30
            THEN 'before_disbursement'  -- Last loan closed >1 month ago
            WHEN li.disbursementdate <= bsd.first_creation_date 
                 AND li.accountstate NOT IN ('ACTIVE', 'ACTIVE_IN_ARREARS')
                 AND DATEDIFF('day', rh.last_repayment_date, bsd.first_creation_date) <= 30
            THEN 'closed_before_creation'  -- Last loan closed <=1 month ago
            WHEN li.disbursementdate > bsd.first_creation_date 
            THEN 'after_savings_creation'  -- Loan created after savings
            ELSE 'other'
        END as loan_status_at_creation
    FROM (
        SELECT 
            client_id,
            MIN(creation_date) as first_creation_date
        FROM base_savings_data
        GROUP BY client_id
    ) bsd
    LEFT JOIN ml.loan_info_tbl li ON bsd.client_id = li.client_id
        AND li.disbursementdate IS NOT NULL
        AND li.ACCOUNTSTATE != 'CLOSED_WRITTEN_OFF'
    LEFT JOIN (
        SELECT 
            loan_id,
            transaction_date as last_repayment_date
        FROM ml.repayment_transactions_extended
        QUALIFY ROW_NUMBER() OVER(PARTITION BY loan_id ORDER BY transaction_date DESC) = 1
    ) rh ON li.loan_id = rh.loan_id
    -- Get the loan that was most recent to savings creation time
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY bsd.client_id 
        ORDER BY 
            CASE 
                WHEN li.disbursementdate IS NULL THEN 0  -- No loan history
                WHEN li.disbursementdate <= bsd.first_creation_date 
                     AND (li.accountstate = 'ACTIVE' OR li.accountstate = 'ACTIVE_IN_ARREARS')
                THEN 1  -- Active loan at creation gets highest priority
                WHEN li.disbursementdate <= bsd.first_creation_date 
                     AND li.accountstate NOT IN ('ACTIVE', 'ACTIVE_IN_ARREARS')
                THEN 2  -- Closed loan before creation gets second priority
                WHEN li.disbursementdate > bsd.first_creation_date 
                THEN 3  -- Future loan gets lowest priority
                ELSE 4
            END,
            li.disbursementdate DESC  -- Most recent loan first
    ) = 1
),

fido_score_at_signup AS (
    SELECT 
        fs.client_id,
        fs.score,
        fs.created_on,
        bsd.first_creation_date
    FROM data.fido_score fs
    JOIN (
        SELECT 
            client_id,
            MIN(creation_date) as first_creation_date
        FROM base_savings_data
        GROUP BY client_id
    ) bsd ON fs.client_id = bsd.client_id
    WHERE fs.created_on <= bsd.first_creation_date
    AND FIDO_SCORE_FLOW != 'FIDOBIZ_SCORE'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY fs.client_id ORDER BY fs.created_on DESC) = 1
),
repayment_history AS (
    SELECT 
        loan_id,
        transaction_date as last_repayment_date,
        repayment_state
    FROM ml.repayment_transactions_extended
    QUALIFY ROW_NUMBER() OVER(PARTITION BY loan_id ORDER BY transaction_date DESC) = 1
),

-- Calculate savings timing relative to loan activity
savings_timing AS (
    SELECT 
        bsd.client_id,
        bsd.first_creation_date as savings_creation_date,
        lasc.loan_id,
        lasc.ln,
        lasc.disbursementdate,
        lasc.accountstate as loan_account_state,
        lasc.loan_status_at_creation,
        rh.last_repayment_date,
        rh.repayment_state,
        -- Calculate days between key events
        DATEDIFF('day', lasc.disbursementdate, bsd.first_creation_date) as days_disbursement_to_savings,
        DATEDIFF('day', rh.last_repayment_date, bsd.first_creation_date) as days_repayment_to_savings,
        -- Determine timing category based on loan status at creation
        CASE 
            WHEN lasc.loan_id IS NULL THEN 'no_loan_history'
            WHEN lasc.loan_status_at_creation = 'active_at_creation' THEN 'with_active_loan'
            WHEN lasc.loan_status_at_creation = 'before_disbursement' THEN 'before_disbursement'
            WHEN lasc.loan_status_at_creation = 'closed_before_creation' THEN 'after_loan_closed'
            ELSE 'other'
        END as savings_timing_category,
        -- Simplified binary categorization
        CASE 
            WHEN lasc.loan_id IS NULL THEN 'no_loan_history'
            WHEN lasc.loan_status_at_creation = 'active_at_creation' THEN 'with_active_loan'
            WHEN lasc.loan_status_at_creation = 'before_disbursement' THEN 'before_disbursement'
            WHEN lasc.loan_status_at_creation = 'closed_before_creation' THEN 'after_loan_closed'
            ELSE 'other'
        END as simplified_timing
    FROM (
        SELECT 
            client_id,
            MIN(creation_date) as first_creation_date
        FROM base_savings_data
        GROUP BY client_id
    ) bsd
    LEFT JOIN loan_at_savings_creation lasc ON bsd.client_id = lasc.client_id
    LEFT JOIN repayment_history rh ON lasc.loan_id = rh.loan_id
    WHERE bsd.first_creation_date IS NOT NULL
),

-- Get first account details for life calculation
first_account_details AS (
    SELECT 
        client_id,
        account_id,
        creation_date as first_account_creation,
        closeddate as first_account_closed_date,
        accountstate as first_account_state,
        DATEDIFF('day', creation_date, COALESCE(closeddate, CURRENT_DATE)) as first_account_life_days
    FROM base_savings_data
    QUALIFY ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY creation_date ASC) = 1
),

-- Client summary with loan and score data
client_summary AS (
    SELECT 
        bsd.client_id,
        COUNT(DISTINCT bsd.account_id) as total_accounts,
        MIN(bsd.creation_date) as first_account_creation,
        MAX(td.transaction_date) as last_transaction_date,
        -- Transaction metrics across all accounts
        COUNT(CASE WHEN td.transaction_type = 'DEPOSIT' THEN 1 END) as total_deposits_count,
        COUNT(CASE WHEN td.transaction_type = 'WITHDRAWAL' THEN 1 END) as total_withdrawals_count,
        SUM(CASE WHEN td.transaction_type = 'DEPOSIT' THEN td.amount ELSE 0 END) as total_deposits_amount,
        SUM(CASE WHEN td.transaction_type = 'WITHDRAWAL' THEN td.amount ELSE 0 END) as total_withdrawals_amount,
        -- Current balance (from most recent transaction)
        MAX(CASE WHEN td.transaction_date = (SELECT MAX(td2.transaction_date) 
                                            FROM transaction_data td2 
                                            WHERE td2.account_key = td.account_key) 
                 THEN td.balance END) as current_balance,
        -- Loan data
        COALESCE(lh.max_ln, -1) as max_ln,
        lh.total_loans,
        -- Fido score data
        fs.score as fido_score_at_signup
    FROM base_savings_data bsd
    LEFT JOIN transaction_data td ON bsd.account_key = td.account_key
    LEFT JOIN loan_history lh ON bsd.client_id = lh.client_id
    LEFT JOIN fido_score_at_signup fs ON bsd.client_id = fs.client_id
    GROUP BY bsd.client_id, lh.max_ln, lh.total_loans, fs.score
),

-- Calculate behavioral segments
behavioral_segments AS (
    SELECT 
        client_id,
        total_deposits_count as deposits,
        total_withdrawals_count as withdrawals,
        current_balance as last_balance,
        first_account_creation,
        last_transaction_date,
        -- Calculate frequency metrics
        deposits / NULLIF(GREATEST(DATEDIFF(MONTH, first_account_creation, last_transaction_date), 1), 0) as monthly_deposit_frequency,
        withdrawals / NULLIF(GREATEST(DATEDIFF(MONTH, first_account_creation, last_transaction_date), 1), 0) as monthly_withdrawal_frequency,
        DATEDIFF('day', first_account_creation, last_transaction_date) as days_active
    FROM client_summary
),

-- Client-level data with segment tags
results as (
SELECT 
    cs.client_id,
    cs.total_accounts,
    cs.total_deposits_count as deposits,
    cs.total_withdrawals_count as withdrawals,
    cs.total_deposits_amount,
    cs.total_withdrawals_amount,
    cs.current_balance as last_balance,
    cs.max_ln,
    cs.total_loans,
    cs.fido_score_at_signup,
   ds.age,
    ds.gender,
    ds.region,
    ds.income_value, 
    ds.cust_location,
    ds.employment,
    ds.marital_status,
    ds.education_level,
    -- Churn and return status
    COALESCE(chs.is_churned, FALSE) as is_churned,
    chs.is_returned,
    chs.churn_indicator_days,
    chs.churn_type,
    -- Loan-based segment
    CASE 
        WHEN cs.max_ln >= 0 AND st.simplified_timing = 'before_disbursement' THEN 'saver_before_ln0'
        WHEN cs.max_ln = -1 and cs.fido_score_at_signup < 250 THEN 'non_eligible_savers fs < 250'
        WHEN cs.max_ln = -1 AND cs.fido_score_at_signup >= 250 THEN 'eligible_no_loan_savers fs > 250 ln = null'
        WHEN lasc.ln = 0 THEN 'ln0_savers'
        WHEN lasc.ln BETWEEN 1 AND 2 THEN 'low_ln_savers ln1-2'
        WHEN lasc.ln BETWEEN 3 AND 7 THEN 'mid_ln_savers ln3-7'
        WHEN lasc.ln >= 8 THEN 'high_ln_savers ln8+'
        ELSE 'unknown'
    END as loan_segment,
    -- Behavioral segment
    CASE 
        WHEN (bs.days_active <= 1 or bs.days_active is null) and  bs.last_balance = 0 THEN 'testers'
        WHEN --(bs.monthly_deposit_frequency >= 10 AND bs.monthly_withdrawal_frequency >= 10) 
             (bs.monthly_deposit_frequency >= 3 AND bs.monthly_withdrawal_frequency >= 3) 
             OR (bs.monthly_withdrawal_frequency >= 3) 
        THEN 'wallet_users monthly deposits and/or withdrawal frequency >=3'
        WHEN bs.last_balance > 400 AND bs.deposits > 1 THEN 'ultra_savers balance > 400'
        WHEN bs.days_active <= 2 and bs.last_balance > 0 then 'new_savers balance > 0, days active <3'
        WHEN bs.last_balance > 0 then 'savers days active >=3, balance < 400 '
        ELSE 'savers_closed'
    END as behavioral_segment,
    fad.first_account_life_days,
    fad.first_account_closed_date,
    fad.first_account_state,
     -- Savings timing analysis
    st.savings_timing_category,
    st.simplified_timing,
    st.days_disbursement_to_savings,
    st.days_repayment_to_savings,
    st.repayment_state,
    -- Primary timing category
    CASE 
        WHEN st.simplified_timing = 'with_active_loan' THEN 'with_active_loan'
        WHEN st.simplified_timing = 'before_disbursement' THEN 'before_disbursement'
        WHEN st.simplified_timing = 'after_loan_closed' THEN 'after_loan_closed'
        WHEN st.simplified_timing = 'no_loan_history' THEN 'no_loan_history'
        ELSE 'other'
    END as primary_timing_category,
    CASE WHEN cs.max_ln = -1 then 'Never Took Loan'
        WHEN cs.max_ln >=0 and lasc.ln is null then 'Savings Before Loan'
        else 'Loan Before Savings'
        end as current_loan_category,
    -- First account life status
    CASE 
        WHEN fad.first_account_state = 'ACTIVE' THEN 'still_active'
        WHEN fad.first_account_closed_date IS NOT NULL THEN 'closed'
        ELSE 'unknown'
    END as first_account_status,
    -- Additional metrics
    bs.monthly_deposit_frequency,
    bs.monthly_withdrawal_frequency,
    bs.days_active
FROM client_summary cs
LEFT JOIN behavioral_segments bs ON cs.client_id = bs.client_id
LEFT JOIN savings_timing st ON cs.client_id = st.client_id
LEFT JOIN loan_at_savings_creation lasc on cs.client_id = lasc.client_id
LEFT JOIN first_account_details fad ON cs.client_id = fad.client_id
LEFT JOIN churn_status chs ON cs.client_id = chs.client_id
LEFT JOIN (
SELECT * from data.survey_data
QUALIFY row_number() over(partition by client_id order by session_id desc) = 1
)ds on ds.client_id = cs.client_id
--where last_balance > 0
ORDER BY cs.client_id)
select *
from results
--group by 1