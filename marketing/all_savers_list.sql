-- List of all savers with segmentation
with savings_details as (
SELECT cl.id as client_id, 
    sa.id as account_id,
    FIRST_VALUE(sa.creationdate) OVER(partition by cl.id order by sa.creationdate asc) as first_creation_date,
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

-- Calculate LN at savings creation
ln_at_signup_calc AS (
  SELECT 
    sa.client_id,
    sa.first_creation_date as signup_date,
    MAX(CASE 
      WHEN li.disbursementdate <= sa.first_creation_date THEN li.LN 
      ELSE null
    END) as ln_at_signup
  FROM savings_details sa
  LEFT JOIN (select * from ml.loan_info_tbl
  WHERE disbursementdate IS NOT NULL
  AND ACCOUNTSTATE != 'CLOSED_WRITTEN_OFF') li ON sa.client_id = li.client_id
  GROUP BY sa.client_id, sa.first_creation_date
),

-- Get clients with no loans (none credit savers) and savings before LN0
savings_client as (
    select cl.id as client_id, 
    sa.creationdate as first_creation_date,
    case when loans.client_id is null then cl.id else null end as without_loan,
    case when loans.client_id is not null then cl.id else null end as with_loan,
    case when loans.client_id is not null and sa.creationdate < li.disbursementdate then cl.id else null end as savings_to_ln0
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
),

-- Get score at signup
loan_details as (
    select 
        sd.client_id,
        sd.account_id,
        sd.creationdate,  
        sd.closeddate as last_modified_date,
        sd.accountstate, 
        fs_at_signup.score as score,
        COALESCE(las.ln_at_signup, null) as ln_at_signup
    from savings_details sd 
    left join (
        select 
            fs.client_id, 
            fs.score, 
            fs.created_on,
            sd.first_creation_date
        from data.fido_score fs
        left join (select distinct client_id, first_creation_date from savings_details) sd
            on fs.client_id = sd.client_id
        where fs.created_on <= sd.first_creation_date
            AND FIDO_SCORE_FLOW != 'FIDOBIZ_SCORE'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY fs.client_id ORDER BY fs.created_on DESC) = 1
    ) fs_at_signup on fs_at_signup.client_id = sd.client_id 
    left join ln_at_signup_calc las ON las.client_id = sd.client_id 
),

-- Apply segmentation
segmented_savers as (
    select 
        ld.client_id,
        ld.account_id,
        ld.creationdate,
        ld.accountstate,
        ld.score,
        ld.ln_at_signup as ln,
        case when ld.ln is null and ld.score < 250 then 'Ineligible New' 
        when ld.ln >=0 and ld.score < 250 then 'Returning Ineligible'
        when ld.ln is null and ld.score >= 250 then 'Eligible New'
        when ld.ln = 0 then 'LN0'
        when ld.ln between 1 and 5 then 'LN1-5'
        when ld.ln between 6 and 9 then 'LN6-9'
        when ld.ln between 10 and 12 then 'LN10-12'
        when ld.ln between 13 and 23 then 'LN13-23'
        else 'LN24+' end as ln_group,
        sc.without_loan,
        sc.with_loan,
        sc.savings_to_ln0
    from loan_details ld
    left join savings_client sc on sc.client_id = ld.client_id
    qualify row_number() over (partition by ld.client_id order by ld.creationdate asc) = 1
)

-- Final output: all savers
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
         when ss.with_loan is not null then 'With Loan'
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
order by ss.client_id

