
-- Filter out loans with NULL disbursement dates first
filtered_loans AS (
    SELECT * FROM ml.loan_info_tbl 
    WHERE disbursementdate is not null
),

last_interaction_loans as(
select li.*, ll.disbursementdate ,ll.ln, ll.loan_product_id , 
case 
    when loan_product_id like '%UCBL%' and ln = 0 then 'FidoBiz New'
    when loan_product_id like '%UCBL%' and ln between 1 and 3 and 
         not exists (
             select 1 from filtered_loans prev_loan 
             where prev_loan.client_id = ll.client_id 
             and prev_loan.loan_product_id like '%UCBL%'
             and prev_loan.ln < ll.ln
         ) then 'FidoBiz Migrated New'
    when loan_product_id like '%UCBL%' and ln > 3 and 
         not exists (
             select 1 from filtered_loans prev_loan 
             where prev_loan.client_id = ll.client_id 
             and prev_loan.loan_product_id like '%UCBL%'
             and prev_loan.ln < ll.ln
         ) then 'FidoBiz Migrated Old'
    when loan_product_id like '%UCBL%' and ln > 0 and 
         exists (
             select 1 from filtered_loans prev_loan 
             where prev_loan.client_id = ll.client_id 
             and prev_loan.loan_product_id like '%UCBL%'
             and prev_loan.ln < ll.ln
         ) then 'FidoBiz Existing'
    else 'Personal' 
end as loan_type
from last_interaction li
left join filtered_loans ll on li.banking_platform_id = ll.client_id 
    and ll.disbursementdate > li.signup_timestamp
where rn = 1
QUALIFY ROW_NUMBER() over(partition by client_id order by LN asc) = 1
)
-- select * from last_interaction_loans
-- where loan_type = 'Migrated New'
WITH 
-- 1. Get USSD users
ussd_users AS (
    SELECT DISTINCT
        u.banking_platform_id,
        u.created_timestamp as signup_timestamp,
        CASE 
            WHEN sl.source LIKE '%998*9%' THEN 'radio_ussd'
            ELSE 'other_ussd'
        END as channel
    FROM GHANA_PROD.BANKING_SERVICE."USER" u
    INNER JOIN banking_service.subscription_log sl 
        ON right(u.PHONE_NUMBER,9) = right(sl.phone_number,9)
    WHERE sl.action = 'opt_in'
        AND sl.source != 'sign_up'
        AND u.banking_platform_id IS NOT NULL
        AND date(u.created_timestamp) BETWEEN date('2025-04-01') AND date('2025-07-31')
),

-- 2. Get Ad users
ad_users AS (
    SELECT DISTINCT
        u.banking_platform_id,
        u.created_timestamp as signup_timestamp,
        'ad' as channel
    FROM GHANA_PROD.BANKING_SERVICE."USER" u
    INNER JOIN (
        SELECT parse_json(IDENTITY_LINK):"fido user id" AS user_id
        FROM KOCHAVA_DATA.INSTALLS
    ) ks ON u.id = ks.user_id
    WHERE u.banking_platform_id IS NOT NULL
        AND date(u.created_timestamp) BETWEEN date('2025-04-01') AND date('2025-07-31')
),

-- 3. Get Organic users (users not in USSD or Ad)
organic_users AS (
    SELECT 
        u.banking_platform_id,
        u.created_timestamp as signup_timestamp,
        'organic' as channel
    FROM GHANA_PROD.BANKING_SERVICE."USER" u
    WHERE date(u.created_timestamp) BETWEEN date('2025-04-01') AND date('2025-07-31')
        AND u.banking_platform_id IS NOT NULL
        AND u.banking_platform_id NOT IN (SELECT banking_platform_id FROM ussd_users)
        AND u.banking_platform_id NOT IN (SELECT banking_platform_id FROM ad_users)
),

-- 4. Combine all users with their channels
all_users AS (
    SELECT * FROM ussd_users
    UNION ALL
    SELECT * FROM ad_users
    UNION ALL
    SELECT * FROM organic_users
),

-- 5. Get loan information for users
user_loans AS (
    SELECT 
        au.banking_platform_id,
        au.signup_timestamp,
        au.channel,
        ll.disbursementdate,
        ll.ln,
        ll.loan_product_id,
        CASE 
            WHEN ll.loan_product_id LIKE '%UCBL%' AND ll.ln = 0 THEN 'FidoBiz New'
            WHEN ll.loan_product_id LIKE '%UCBL%' AND ll.ln BETWEEN 1 AND 3 THEN 'FidoBiz Migrated New'
            WHEN ll.loan_product_id LIKE '%UCBL%' AND ll.ln > 3 THEN 'FidoBiz Migrated Old'
            WHEN ll.loan_product_id LIKE '%UCBL%' THEN 'FidoBiz Existing'
            WHEN ll.loan_product_id IS NOT NULL THEN 'Personal'
            ELSE 'No Loan'
        END as loan_type
    FROM all_users au
    LEFT JOIN ml.loan_info_tbl ll 
        ON au.banking_platform_id = ll.client_id 
        AND ll.disbursementdate > au.signup_timestamp
        AND ll.disbursementdate IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY ll.client_id ORDER BY ll.ln ASC) = 1
)

-- 6. Final output
SELECT 
    date(ul.signup_timestamp) as signup_date,
    ul.channel,
    ul.loan_type,
    COUNT(DISTINCT CASE WHEN ul.channel = 'organic' THEN ul.banking_platform_id END) as organic_users,
    COUNT(DISTINCT CASE WHEN ul.channel = 'ad' THEN ul.banking_platform_id END) as ad_users,
    COUNT(DISTINCT CASE WHEN ul.channel = 'radio_ussd' THEN ul.banking_platform_id END) as radio_ussd_users,
    COUNT(DISTINCT CASE WHEN ul.channel = 'other_ussd' THEN ul.banking_platform_id END) as other_ussd_users,
    COUNT(DISTINCT CASE WHEN ul.loan_type = 'FidoBiz New' THEN ul.banking_platform_id END) as fidobiz_new_loans,
    COUNT(DISTINCT CASE WHEN ul.loan_type = 'FidoBiz Migrated New' THEN ul.banking_platform_id END) as fidobiz_migrated_new_loans,
    COUNT(DISTINCT CASE WHEN ul.loan_type = 'FidoBiz Migrated Old' THEN ul.banking_platform_id END) as fidobiz_migrated_old_loans,
    COUNT(DISTINCT CASE WHEN ul.loan_type = 'FidoBiz Existing' THEN ul.banking_platform_id END) as fidobiz_existing_loans,
    COUNT(DISTINCT CASE WHEN ul.loan_type = 'Personal' THEN ul.banking_platform_id END) as personal_loans,
    COUNT(DISTINCT CASE WHEN ul.loan_type = 'No Loan' THEN ul.banking_platform_id END) as no_loan_users,
    COUNT(DISTINCT ul.banking_platform_id) as total_users
FROM user_loans ul
GROUP BY 1, 2, 3
ORDER BY signup_date, channel;