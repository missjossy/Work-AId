-- List of clients who are "none credit savers" (no loans) OR "saving before Ln0" (savings before first loan)
with savings_details as (
SELECT cl.id as client_id, 
    sa.id as account_id,
    sa.creationdate,
    sa.accountstate,
    sa.closeddate
FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa
LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
LEFT JOIN GHANA_PROD.MAMBU.CUSTOMFIELDVALUE wallet_id ON sa.ENCODEDKEY = wallet_id.PARENTKEY AND wallet_id.CUSTOMFIELDKEY = (SELECT encodedkey FROM GHANA_PROD.MAMBU.CUSTOMFIELD WHERE id = 'ACCOUNT_NUMBER_TRANSACTION_CHANN')
where sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
and (cl.MOBILEPHONE1 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE1 is null)
and (cl.MOBILEPHONE2 not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or cl.MOBILEPHONE2 is null) 
and (wallet_id.value not in ('233552602681', '233243630046', '233245822584' , '23346722591', '233257806345') or wallet_id.value is null)
AND date(sa.creationdate) >= date('2025-04-03')
AND sa.accountstate != 'WITHDRAWN'
),

-- Get first creation date per client
first_savings as (
    select 
        client_id,
        min(creationdate) as first_creation_date
    from savings_details
    group by client_id
),

-- Calculate LN at savings creation
ln_at_signup_calc AS (
  SELECT 
    fs.client_id,
    fs.first_creation_date as signup_date,
    MAX(CASE 
      WHEN li.disbursementdate <= fs.first_creation_date THEN li.LN 
      ELSE null
    END) as ln_at_signup
  FROM first_savings fs
  LEFT JOIN (select * from ml.loan_info_tbl
  WHERE disbursementdate IS NOT NULL
  AND ACCOUNTSTATE != 'CLOSED_WRITTEN_OFF') li ON fs.client_id = li.client_id
  GROUP BY fs.client_id, fs.first_creation_date
),

-- Get clients with no loans (none credit savers) and savings before LN0
savings_client as (
    select 
        fs.client_id,
        fs.first_creation_date,
        case when loans.client_id is null then fs.client_id else null end as without_loan,
        case when loans.client_id is not null then fs.client_id else null end as with_loan,
        case when loans.client_id is not null and fs.first_creation_date < li.disbursementdate then fs.client_id else null end as savings_to_ln0
    from first_savings fs
    LEFT JOIN (SELECT DISTINCT CLIENT_ID FROM GHANA_PROD.ML.LOAN_INFO_TBL WHERE DISBURSEMENTDATE IS NOT NULL) loans ON fs.client_id = loans.CLIENT_ID
    LEFT JOIN (
        select * from ml.loan_info_tbl
        where disbursementdate is not null
        qualify row_number() over(partition by client_id order by disbursementdate asc) = 1
    ) li on li.client_id = fs.client_id
),

-- Get score at signup
loan_details as (
    select 
        fs.client_id,
        fs.first_creation_date as creationdate,
        fs_at_signup.score as score,
        COALESCE(las.ln_at_signup, null) as ln_at_signup
    from first_savings fs 
    left join (
        select 
            fs_score.client_id, 
            fs_score.score, 
            fs_score.created_on,
            fs.first_creation_date
        from data.fido_score fs_score
        left join first_savings fs on fs_score.client_id = fs.client_id
        where fs_score.created_on <= fs.first_creation_date
            AND FIDO_SCORE_FLOW != 'FIDOBIZ_SCORE'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY fs_score.client_id ORDER BY fs_score.created_on DESC) = 1
    ) fs_at_signup on fs_at_signup.client_id = fs.client_id 
    left join ln_at_signup_calc las ON las.client_id = fs.client_id 
),

-- Apply segmentation
segmented_savers as (
    select 
        ld.client_id,
        ld.creationdate,
        ld.score,
        ld.ln_at_signup as ln,
        case when ld.ln is null and (ld.score < 250 or ld.score is null) then 'Ineligible New' 
        when ld.ln >=0 and (ld.score < 250 or ld.score is null) then 'Returning Ineligible'
        when ld.ln is null and ld.score >= 250 then 'Eligible New'
        when ld.ln = 0 then 'LN0'
        when ld.ln between 1 and 5 then 'LN1-5'
        when ld.ln between 6 and 9 then 'LN6-9'
        when ld.ln between 10 and 12 then 'LN10-12'
        when ld.ln between 13 and 23 then 'LN13-23'
        else 'LN24+' end as ln_group,
        sc.without_loan,
        sc.savings_to_ln0
    from loan_details ld
    left join savings_client sc on sc.client_id = ld.client_id
)

-- Final output: clients who are none credit savers OR saving before Ln0
select 
    ss.client_id,
    fs.score as current_score,
    li.accountstate,
    li.LN,
    ds.firstname,
    ds.lastname,
    ds.gender,
    ds.age,
    ds.industry,
    ds.position,
    ds.mobilephone,
    ds.altphone,
    ds.region,
    ss.ln_group,
    case when ss.without_loan is not null then 'None Credit Saver' 
         when ss.savings_to_ln0 is not null then 'Savings Before LN0'
         else 'Other' end as saver_type
from segmented_savers ss
left join (
    select * from ml.loan_info_tbl
    qualify row_number() over(partition by client_id order by disbursementdate desc, LN desc) = 1
) li on li.client_id = ss.client_id
left join (
    select * 
    from data.fido_score
    qualify row_number() over(partition by client_id order by created_on desc) = 1
) fs on ss.client_id = fs.client_id
left join (
    select * from data.survey_data
    qualify row_number() over(partition by client_id order by loan_date desc) = 1
) ds on ds.client_id = ss.client_id
where (ss.without_loan is not null OR ss.savings_to_ln0 is not null)
order by ss.client_id

-- Uncomment below to debug - check if segmented_savers has data
-- select count(*) as total_segmented, 
--        count(without_loan) as count_without_loan,
--        count(savings_to_ln0) as count_savings_to_ln0
-- from segmented_savers

