
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
savings_client as(

    select cl.id as client_id, 
    sa.creationdate as first_creation_date,
    case when loans.client_id is null then cl.id else null end as without_loan,
    case when loans.client_id is not null then cl.id else null end as with_loan,
    case when loans.client_id is not null and first_creation_date < li.disbursementdate then cl.id else null end as savings_to_ln0
    FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa
         LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
        LEFT JOIN (SELECT DISTINCT CLIENT_ID FROM GHANA_PROD.ML.LOAN_INFO_TBL WHERE DISBURSEMENTDATE IS NOT NULL) loans ON cl.ID = loans.CLIENT_ID
        LEFT JOIN (
        select * from ml.loan_info_tbl
        where disbursementdate is not null
        qualify row_number() over(partition by client_id order by disbursementdate asc) = 1
        ) li on li.client_id = cl.id
        LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_id ON sa.ENCODEDKEY = wallet_id.PARENTKEY AND wallet_id.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'ACCOUNT_NUMBER_TRANSACTION_CHANN')
        WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
        AND DATE(sa.creationdate) >= date('2025-04-03')
        AND (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
        AND (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE2 is null)
        AND (wallet_id.value not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or wallet_id.value is null)
        AND sa.accountstate != 'WITHDRAWN'
        qualify row_number() over (partition by cl.id order by sa.creationdate asc) = 1
        order by client_id
        --limit 1000
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


savings_loan_sum as (
select date_trunc('{{sacle}}', first_creation_date) as date_,
count( with_loan) as with_ln0,
count(without_loan) as without_ln0,
count(savings_to_ln0) as savings_before_ln0
from savings_client
where date_ BETWEEN date('{{ Range.start }}') and date('{{ Range.end }}')
group by 1
)
-- select * from savings_loan_sum
-- where date_ BETWEEN date('{{ Range.start }}') and date('{{ Range.end }}')
-- select distinct client_id, first_creation_date, without_loan, with_loan
-- from savings_data
-- order by client_id
-- limit 1000

SELECT 
    sign_ups.date_ as time,
    COALESCE(new_savers.new_daily_clients, 0) as "New Savers",
    COALESCE(returning_savers.returning_daily_clients, 0) as "Returning Savers",
    COALESCE(active_users_by_period.active_accounts, 0) as "Active Users",
    CASE WHEN COALESCE(active_users_by_period.active_accounts, 0) > 0 
         THEN savings_without_loans_by_period.accounts_no_loans / active_users_by_period.active_accounts * 100 
         ELSE 0 END as "% Active Users with Savings Only",
    CASE WHEN COALESCE(cumulative_signups.cumulative_signups, 0) > 0 
         THEN ln0_loan_only.loan_only_clients / cumulative_signups.cumulative_signups * 100 
         ELSE 0 END as "% Signups with LN0 Only",
    COALESCE(with_ln0, 0) as "With LN0 Disbursement",
    COALESCE(without_ln0, 0) as "Without LN0 Disbursement",
    coalesce(savings_before_ln0, 0) as "Savings Before LN0",
    CASE WHEN COALESCE(new_savers.new_daily_clients, 0) > 0 
         THEN (savings_before_ln0 / new_savers.new_daily_clients) * 100 
         ELSE 0 END as "% Savings Before LN0"
FROM sign_ups 
LEFT JOIN new_savers ON new_savers.date_ = sign_ups.date_
LEFT JOIN returning_savers ON returning_savers.date_ = sign_ups.date_
LEFT JOIN active_users_by_period ON active_users_by_period.date_ = sign_ups.date_
LEFT JOIN savings_without_loans_by_period ON savings_without_loans_by_period.date_ = sign_ups.date_
LEFT JOIN cumulative_signups ON cumulative_signups.date_ = sign_ups.date_
LEFT JOIN ln0_loan_only ON ln0_loan_only.date_ = sign_ups.date_
LEFT JOIN savings_loan_sum ON savings_loan_sum.date_ = sign_ups.date_
ORDER BY time DESC