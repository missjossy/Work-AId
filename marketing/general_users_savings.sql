select cl.client_id,fs.score as current_score,li.accountstate, li.LN, ds.firstname, ds.lastname, ds.gender, ds.age, ds.industry,ds.position, ds.mobilephone, ds.altphone,ds.region 
from ml.client_info cl
left join (
select * from ml.loan_info_tbl
qualify row_number() over(partition by client_id order by disbursementdate, LN desc) = 1
) li on li.client_id = cl.client_id
left join (
select * 
from data.fido_score
qualify row_number() over(partition by client_id order by created_on desc) =1
) fs on cl.client_id = fs.client_id
left join (
select * from data.survey_data
qualify row_number() over(partition by client_id order by loan_date desc) = 1
) ds on ds.client_id = cl.client_id
where cl.client_id not in (
select distinct cl.id 
FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa
LEFT JOIN MAMBU.CLIENT cl on cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
and accountstate != 'WITHDRAWN'
)
and BUSINESS_RULE_DECISION = 'APPROVED'
and fs.score > ({{score_limit}})
and (fraud_type_matched = '' or fraud_type_matched is null)
and date(fs.created_on) between  date('{{ Range.start }}') and  date('{{ Range.end }}')
and li.accountstate != 'CLOSED_WRITTEN_OFF'
and li.accountstate in ({{accountstates}})