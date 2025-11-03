
WITH savings_data as (
select  cl.id as client_id, 
    min(sa.creationdate) OVER(partition by cl.id) as first_creation_date,
    case when loans.client_id is null then 1 else 0 end as without_loan,
    case when loans.client_id is not null then 1 else 0 end as with_loan,
    sa.id as account_id , sa.creationdate
        FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa
         LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
        LEFT JOIN (SELECT DISTINCT CLIENT_ID FROM GHANA_PROD.ML.LOAN_INFO_TBL WHERE DISBURSEMENTDATE IS NOT NULL) loans ON cl.ID = loans.CLIENT_ID
        LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_id ON sa.ENCODEDKEY = wallet_id.PARENTKEY AND wallet_id.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'ACCOUNT_NUMBER_TRANSACTION_CHANN')
        WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
         AND DATE(sa.creationdate) >= date('2025-04-03')
        AND (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
        AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE2 is null)
        AND (wallet_id.value not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or wallet_id.value is null)
        AND sa.accountstate != 'WITHDRAWN'
),
-- Get all users who signed up in the date range
signup_users as (
    SELECT 
        date_trunc('{{sacle}}', u.CREATED_TIMESTAMP) as signup_date,
        u.BANKING_PLATFORM_ID as client_id
    FROM GHANA_PROD.BANKING_SERVICE."USER" u
    WHERE date(u.CREATED_TIMESTAMP) BETWEEN '{{ Range.start }}' and '{{ Range.end }}'
),

-- Get first savings account creation date for each client
first_savings as (
    SELECT 
        cl.id as client_id,
        MIN(sa.creationdate) as first_savings_date
    FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa
    LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
    LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_id ON sa.ENCODEDKEY = wallet_id.PARENTKEY AND wallet_id.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'ACCOUNT_NUMBER_TRANSACTION_CHANN')
    WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
        AND sa.accountstate != 'WITHDRAWN'
        AND DATE(sa.creationdate) >= date('2025-04-03')
        AND (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584', '23346722591', '233257806345') OR cl.MOBILEPHONE1 is null)
        AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584', '23346722591', '233257806345') OR cl.MOBILEPHONE2 is null)
        AND (wallet_id.value not in ('233552602681', '233243630046', '233245822584', '23346722591', '233257806345') OR wallet_id.value is null)
    GROUP BY cl.id
),

-- Get first loan disbursement date for each client
first_loan as (
    SELECT 
        CLIENT_ID,
        MIN(DISBURSEMENTDATE) as first_loan_date
    FROM GHANA_PROD.ML.LOAN_INFO_TBL
    WHERE DISBURSEMENTDATE IS NOT NULL
    GROUP BY CLIENT_ID
),

new_savers AS ( 
SELECT 
date_trunc('{{sacle}}', first_creation_date) date_ , 
count(distinct client_id) as new_daily_clients, 
count(distinct account_id)  as new_daily_accounts 
from savings_data
where date(first_creation_date) BETWEEN date('{{ Range.start }}') and date('{{ Range.end }}')
group by date_ 
order by date_), 

returning_savers AS (
SELECT 
date_trunc('{{sacle}}', creationdate) date_ , 
count(distinct client_id) as returning_daily_clients, 
count(distinct account_id)  as returning_daily_accounts 
from savings_data
where date(creationdate) BETWEEN date('{{ Range.start }}') and date('{{ Range.end }}')
  AND first_creation_date < creationdate
group by date_ 
order by date_),

date_range as (SELECT DATE('{{ Range.start }}') AS date_
    UNION ALL
    SELECT DATEADD(DAY, 1, date_)
    FROM date_range
    WHERE date_ < DATE('{{ Range.end }}')
), 

active_accounts_by_date as (
    SELECT 
        d.date_ as report_date,
        COUNT(DISTINCT sa.id) as active_accounts
    FROM date_range d
    JOIN GHANA_PROD.MAMBU.SAVINGSACCOUNT sa ON 
        DATE(sa.creationdate) <= d.date_
        AND sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
        AND sa.accountstate != 'WITHDRAWN'
        AND DATE(sa.creationdate) >= DATE('2025-04-03')
        AND ((sa.ACCOUNTSTATE = 'ACTIVE') OR (sa.ACCOUNTSTATE = 'CLOSED' AND DATE(sa.closeddate) > d.date_))
    LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
    LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_id ON 
        sa.ENCODEDKEY = wallet_id.PARENTKEY AND 
        wallet_id.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'ACCOUNT_NUMBER_TRANSACTION_CHANN')
    WHERE (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584', '23346722591', '233257806345') 
           OR cl.MOBILEPHONE1 is null)
        AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584', '23346722591', '233257806345') 
             OR cl.MOBILEPHONE2 is null) 
        AND (wallet_id.value not in ('233552602681', '233243630046', '233245822584', '23346722591', '233257806345') 
             OR wallet_id.value is null)
    GROUP BY 1
),

active_users_by_period as (
    SELECT 
        date_trunc('{{sacle}}', report_date) AS date_,
        MAX(active_accounts) as active_accounts
    FROM active_accounts_by_date
    GROUP BY date_trunc('{{sacle}}', report_date)
),

savings_without_loans_by_period as (
    SELECT 
        date_trunc('{{sacle}}', report_date) AS date_,
        MAX(active_accounts) as accounts_no_loans
    FROM (
        SELECT 
            d.date_ as report_date,
            COUNT(DISTINCT sa.id) as active_accounts
        FROM date_range d
        JOIN GHANA_PROD.MAMBU.SAVINGSACCOUNT sa ON 
            DATE(sa.creationdate) <= d.date_
            AND sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
            AND sa.accountstate != 'WITHDRAWN'
            AND DATE(sa.creationdate) >= DATE('2025-04-03')
            AND ((sa.ACCOUNTSTATE = 'ACTIVE') OR (sa.ACCOUNTSTATE = 'CLOSED' AND DATE(sa.closeddate) > d.date_))
        LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
        LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_id ON 
            sa.ENCODEDKEY = wallet_id.PARENTKEY AND 
            wallet_id.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'ACCOUNT_NUMBER_TRANSACTION_CHANN')
        LEFT JOIN (SELECT DISTINCT CLIENT_ID FROM GHANA_PROD.ML.LOAN_INFO_TBL WHERE DISBURSEMENTDATE IS NOT NULL) loans ON cl.ID = loans.CLIENT_ID
        WHERE (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584', '23346722591', '233257806345') OR cl.MOBILEPHONE1 is null)
            AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584', '23346722591', '233257806345') OR cl.MOBILEPHONE2 is null) 
            AND (wallet_id.value not in ('233552602681', '233243630046', '233245822584', '23346722591', '233257806345') OR wallet_id.value is null)
            AND loans.CLIENT_ID IS NULL
        GROUP BY 1
    )
    GROUP BY date_trunc('{{sacle}}', report_date)
),

sign_ups as (
    select date_trunc('{{sacle}}', CREATED_TIMESTAMP) date_, count(*) as sign_ups
    from (
        SELECT DISTINCT CREATED_TIMESTAMP, USER.ID
        FROM GHANA_PROD.BANKING_SERVICE."USER" 
        WHERE date(CREATED_TIMESTAMP) BETWEEN '{{ Range.start }}' and '{{ Range.end }}'
    )
    group by date_
    order by date_
),

cumulative_signups as (
    SELECT 
        date_,
        SUM(sign_ups) OVER (ORDER BY date_) as cumulative_signups
    FROM sign_ups
),

ln0_loan_only_daily as (
    SELECT 
        date_trunc('{{sacle}}', ml.DISBURSEMENTDATE) as date_,
        COUNT(DISTINCT ml.CLIENT_KEY) as loan_only_clients
    FROM GHANA_PROD.ML.LOAN_INFO_TBL ml
    LEFT JOIN (
        SELECT DISTINCT cl.id as client_id
FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa 
LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
        LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_id ON 
            sa.ENCODEDKEY = wallet_id.PARENTKEY AND 
            wallet_id.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'ACCOUNT_NUMBER_TRANSACTION_CHANN')
        WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS' 
        AND sa.accountstate != 'WITHDRAWN'
        AND (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584', '23346722591', '233257806345') OR cl.MOBILEPHONE1 is null)
        AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584', '23346722591', '233257806345') OR cl.MOBILEPHONE2 is null) 
        AND (wallet_id.value not in ('233552602681', '233243630046', '233245822584', '23346722591', '233257806345') OR wallet_id.value is null)
    ) savings ON ml.CLIENT_ID = savings.client_id
    WHERE ml.LN = 0 
        AND ml.DISBURSEMENTDATE IS NOT NULL
        AND savings.client_id IS NULL
        AND date_trunc('{{sacle}}', ml.DISBURSEMENTDATE) <= date_trunc('{{sacle}}', DATE('{{ Range.end }}'))
    GROUP BY 1
),

ln0_loan_only as (
    SELECT 
        date_,
        SUM(loan_only_clients) OVER (ORDER BY date_) as loan_only_clients
    FROM ln0_loan_only_daily
    WHERE date_ >= date_trunc('{{sacle}}', DATE('{{ Range.start }}'))
),


-- Get first loan disbursement date for each client
first_loan as (
    SELECT 
        CLIENT_ID,
        MIN(DISBURSEMENTDATE) as first_loan_date
    FROM GHANA_PROD.ML.LOAN_INFO_TBL
    WHERE DISBURSEMENTDATE IS NOT NULL
    GROUP BY CLIENT_ID
),

-- Calculate metrics for those who created savings accounts on each date
savings_metrics as (
    SELECT 
        date_trunc('{{sacle}}', sd.creationdate) as creation_date,
        COUNT(DISTINCT CASE WHEN sd.first_creation_date = sd.creationdate THEN sd.client_id END) as new_savers,
        COUNT(DISTINCT CASE WHEN sd.first_creation_date < sd.creationdate THEN sd.client_id END) as returning_savers,
        COUNT(DISTINCT CASE 
            WHEN sd.first_creation_date = sd.creationdate 
            AND (fl.first_loan_date IS NULL OR fl.first_loan_date > sd.creationdate)
            THEN sd.client_id 
        END) as savers_without_loan,
        COUNT(DISTINCT CASE 
            WHEN sd.first_creation_date = sd.creationdate 
            AND fl.first_loan_date IS NOT NULL 
            THEN sd.client_id 
        END) as savers_with_loan,
        COUNT(DISTINCT CASE WHEN sd.first_creation_date = sd.creationdate 
            AND fl.first_loan_date IS NOT NULL 
            AND sd.creationdate < fl.first_loan_date
            THEN sd.client_id 
        END) as savings_first_then_loan
    FROM savings_data sd
    LEFT JOIN first_loan fl ON sd.client_id = fl.CLIENT_ID
    WHERE date(sd.creationdate) BETWEEN date('{{ Range.start }}') and date('{{ Range.end }}')
    GROUP BY date_trunc('{{sacle}}', sd.creationdate)
),

sign_ups as (
    select date_trunc('{{sacle}}', CREATED_TIMESTAMP) date_, count(*) as sign_ups
    from (
        SELECT DISTINCT CREATED_TIMESTAMP, USER.ID
        FROM GHANA_PROD.BANKING_SERVICE."USER" 
        WHERE date(CREATED_TIMESTAMP) BETWEEN '{{ Range.start }}' and '{{ Range.end }}'
    )
    group by date_
    order by date_
),

cumulative_signups as (
    SELECT 
        date_,
        SUM(sign_ups) OVER (ORDER BY date_) as cumulative_signups
    FROM sign_ups
)

SELECT 
    sm.creation_date as time,
    sm.new_savers as "New Savers",
    sm.returning_savers as "Returning Savers",
    au.active_accounts as "Active Users",
    CASE WHEN au.active_accounts > 0 
         THEN sw.accounts_no_loans / au.active_accounts * 100 
         ELSE 0 END as "% Active Users with Savings Only",
    CASE WHEN cs.cumulative_signups > 0 
         THEN lo.loan_only_clients / cs.cumulative_signups * 100 
         ELSE 0 END as "% Signups with LN0 Only",
    sm.savers_without_loan as "Savers Without Loan",
    sm.savers_with_loan as "Savers With Loan",
    sm.savings_first_then_loan as "Savings First Then Loan"
FROM savings_metrics sm
LEFT JOIN active_users_by_period au ON au.date_ = sm.creation_date
LEFT JOIN savings_without_loans_by_period sw ON sw.date_ = sm.creation_date
LEFT JOIN cumulative_signups cs ON cs.date_ = sm.creation_date
LEFT JOIN ln0_loan_only lo ON lo.date_ = sm.creation_date
ORDER BY sm.creation_date DESC