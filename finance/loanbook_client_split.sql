WITH payments as (
select to_char(month,'YYYY-MM') month,
      m.LN,
      sum(principal_change) principal_paid, 
      sum(interestamount_true) interest, 
      sum(feesamount_true) feess, 
      sum(penaltyamount_true) penalty, 
      sum(old_processing_fee) processing_fee
from(
    select month,
           m.loan_key,    
           m.loanamount,
           m.LN,
           l.transactionid,
           l.principalbalance,
           case when l.amount < 0 then -interestamount else interestamount end as interestamount_true,
           case when l.amount < 0 then -feesamount else feesamount end as feesamount_true,
           case when l.amount < 0 then -penaltyamount else penaltyamount end as penaltyamount_true,
           case when l.amount < 0 then -principalamount else principalamount end as principalamount_true,
           case when principalamount_true > 0 then
                    case when l.principalbalance >= m.loanamount then principalamount_true
                         when l.principalbalance < m.loanamount then greatest(l.principalbalance + principalamount_true - m.loanamount, 0)
                    end
                when principalamount_true < 0 then
                    case when l.principalbalance < m.loanamount then 0
                         when l.principalbalance >= m.loanamount then greatest(principalamount_true,-l.principalbalance + m.loanamount)
                    end
                else 0
            end old_processing_fee,
           principalamount_true - old_processing_fee principal_change
    from GHANA_PROD.ML.LOAN_INFO_TBL m 
    join GHANA_PROD.MAMBU.loantransaction l on m.loan_key = l.parentaccountkey
    join (select distinct date_trunc('month',creationdate) month from GHANA_PROD.MAMBU.loantransaction where date(creationdate) between '2024-01-01' and '2025-09-01') days on date_trunc('month',l.creationdate) = days.month
    where l."type" in ('REPAYMENT_ADJUSTMENT','REPAYMENT')           
    )
group by 1, 2
),

disbursements as
(select to_char(l.creationdate,'YYYY-MM') month, 
        m.LN,
        count(distinct m.client_id) clients,
        sum(case when date(l.creationdate) <= '2022-07-05' then l.amount else l.principalamount end) disbursed,
        sum(case when date(l.creationdate) > '2022-07-05' then l.principalamount -l.amount else 0 end) commmitment_fe
from GHANA_PROD.MAMBU.loantransaction l
join (select distinct date_trunc('month',creationdate) month from GHANA_PROD.MAMBU.loantransaction where date(creationdate) between '2024-01-01' and '2025-09-01') days on date_trunc('month',l.creationdate) = days.month 
left join GHANA_PROD.ml.LOAN_INFO_TBL m on  m.loan_key = l.parentaccountkey
where l."type" in ('DISBURSMENT','DISBURSMENT_ADJUSTMENT')
group by 1, 2
),

payments_by_segment as (
    select 
        month,
        sum(case when LN = 0 then interest else 0 end) as interest_ln0,
        sum(case when LN > 0 then interest else 0 end) as interest_returning,
        sum(case when LN = 0 then feess + processing_fee else 0 end) as fees_ln0,
        sum(case when LN > 0 then feess + processing_fee else 0 end) as fees_returning,
        sum(case when LN = 0 then penalty else 0 end) as penalty_ln0,
        sum(case when LN > 0 then penalty else 0 end) as penalty_returning
    from payments
    group by 1
),

disbursements_by_segment as (
    select 
        month,
        sum(case when LN = 0 then commmitment_fe else 0 end) as commitment_fee_ln0,
        sum(case when LN > 0 then commmitment_fe else 0 end) as commitment_fee_returning
    from disbursements
    group by 1
)

select 
    p.month,
    -- LN0 Revenue
    p.interest_ln0 + p.fees_ln0 + p.penalty_ln0 + d.commitment_fee_ln0 as Gross_Revenue_LN0,
    p.interest_ln0 + p.fees_ln0 + p.penalty_ln0 as Revenue_Minus_CommitmentFee_LN0,
    -- Returning Revenue
    p.interest_returning + p.fees_returning + p.penalty_returning + d.commitment_fee_returning as Gross_Revenue_Returning,
    p.interest_returning + p.fees_returning + p.penalty_returning as Revenue_Minus_CommitmentFee_Returning
from payments_by_segment p
left join disbursements_by_segment d on p.month = d.month
order by p.month
