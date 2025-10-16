select PARSE_JSON(payment_details):banking_platform_id::integer as client_id, date(tm.created_on) as created_date,
PARSE_JSON(payment_details):walletId::integer as phone_number,
LTRIM(SUBSTRING(PARSE_JSON(payment_details):correlationId::string, REGEXP_INSTR(PARSE_JSON(payment_details):correlationId::string, '\\d'),
    LENGTH(PARSE_JSON(payment_details):correlationId::string)
  )) as corr_id,
tm.ID as payment_id, amount as payment_amount, ts.STATE, ts.SUCCESS ,payment_details, ts.description
from money_transfer.transaction_metadata_p tm
left join money_transfer.transaction_states_p ts on tm.ID = ts.ID
where transaction_type ='DEPOSIT'
and PARSE_JSON(PAYMENT_DETAILS):paymentType::string in ('MAIN', 'BUSINESS')
and SUCCESS = 'FALSE'
limit 100

-- select * from money_transfers.transactions
-- where transaction_id = '1234567890'

