select *,
case 
  -- FidoBiz KYB
  when text like '%FidoBiz offers up to GHS 8,200. Upload a valid document%' 
    or text like '%Want extra%cash? FidoBiz offers up to GHS 8,200. Upload a valid document%' then 'FidoBiz KYB'
  
  -- KYC
  when text like '%Finish your signup for up to GHS 8,200 with Fido%' 
    or text like '%Finish your signup for up to GHS 8,200 with FIDO%' then 'KYC'
  
  -- Non-eligible to Eligible
  when text like '%Upload a MoMo statement for your loan approval%' 
    or text like '%Upload a MoMo statement for your FIDO loan approval%' then 'Non-eligible to Eligible'
  
  -- Savings
  when text like '%EasySave%' 
    or text like '%b.fido.money/save%' 
    or text like '%10% interest%' 
    or text like '%Sign up for EasySave%' 
    or text like '%Sign up for FIDO''s EasySave%' 
    or text like '%Sign up for Fido''s EasySave%' 
    or text like '%Park your funds in EasySave%' 
    or text like '%EasySave grows%' then 'Savings'
  
  -- USSD Dropoff
  when text like '%FIDO offers up to GHS 8,200 in a few simple steps%' then 'USSD Dropoff'
  
  -- KYC USSD
  when text like '%Dial *998*88#%' 
    or text like '%Quickly verify your ID with MTN & FIDO. Dial *998*88#%' then 'KYC USSD'
  
  -- Referrals (general)
  when text like '%Make money from referrals. Earn GHS 55%' 
    or text like '%app.fido.money/invite-friends%' then 'Referrals'
  
  -- FidoBiz Referrals
  when text like '%Make good cash! Get GHS 55 per referral%' 
    or text like '%FidoBiz Referral Champions%' then 'FidoBiz Referrals'
  
  -- FidoBiz Approved
  when text like '%You''re already approved for an instant FidoBiz loan%' then 'FidoBiz Approved'
  
  -- Eligible
  when text like '%You are already eligible for a FIDO loan%' then 'Eligible'
  
  -- FidoBiz LPs
  when text like '%Thousands of people are cashing out their FidoBiz loan%' 
    or text like '%Landing Page to App Dropoffs%' then 'FidoBiz LPs'
  
  -- FidoBiz Cross Sell
  when text like '%Upgrade for more cash! You already qualify for more with FidoBiz%' 
    or text like '%Upgrade to FidoBiz for more cash%' 
    or text like '%Your FidoBiz upgrade gets you more cash%' 
    or text like '%Get extra. Step things up and get up to GHS 8,200 with FidoBiz%' 
    or text like '%FidoBiz Cross Sell%' then 'FidoBiz Cross Sell'
  
  -- FidoBiz Non-eligible to Eligible
  when text like '%Take this chance! Get back in with a valid document%' 
    or (text like '%Upload a valid document (like MoMo statement) for up to GHS 8,200%' 
        and text like '%FidoBiz%') then 'FidoBiz Non-eligible to Eligible'
  
  else null
end as campaign_name,

case 
  -- FidoBiz KYB
  when text like '%FidoBiz offers up to GHS 8,200. Upload a valid document%' 
    or text like '%Want extra%cash? FidoBiz offers up to GHS 8,200. Upload a valid document%' then 'FidoBiz KYB without Doc Submitted'
  
  -- KYC
  when text like '%Finish your signup for up to GHS 8,200 with Fido%' 
    or text like '%Finish your signup for up to GHS 8,200 with FIDO%' then 'KYC verified with out Survey Completion'
  
  -- Non-eligible to Eligible
  when text like '%Upload a MoMo statement for your loan approval%' 
    or text like '%Upload a MoMo statement for your FIDO loan approval%' then 'Potential Non-Eligible'
  
  -- Savings
  when text like '%EasySave%' 
    or text like '%b.fido.money/save%' 
    or text like '%10% interest%' 
    or text like '%Sign up for EasySave%' 
    or text like '%Sign up for FIDO''s EasySave%' 
    or text like '%Sign up for Fido''s EasySave%' 
    or text like '%Park your funds in EasySave%' 
    or text like '%EasySave grows%' then 'EasySave Blast'
  
  -- USSD Dropoff
  when text like '%FIDO offers up to GHS 8,200 in a few simple steps%' then 'USSD Dropoffs'
  
  -- KYC USSD
  when text like '%Dial *998*88#%' 
    or text like '%Quickly verify your ID with MTN & FIDO. Dial *998*88#%' then 'KYC USSD Boost'
  
  -- Referrals (general)
  when text like '%Make money from referrals. Earn GHS 55%' 
    or text like '%app.fido.money/invite-friends%' then 'Referral Campaign'
  
  -- FidoBiz Referrals
  when text like '%Make good cash! Get GHS 55 per referral%' 
    or text like '%FidoBiz Referral Champions%' then 'FidoBiz Referral Champions'
  
  -- FidoBiz Approved
  when text like '%You''re already approved for an instant FidoBiz loan%' then 'FidoBiz Approved but no L0'
  
  -- Eligible
  when text like '%You are already eligible for a FIDO loan%' then 'Eligible but no L0'
  
  -- FidoBiz LPs
  when text like '%Thousands of people are cashing out their FidoBiz loan%' 
    or text like '%Landing Page to App Dropoffs%' then 'Landing Page to App Dropoffs'
  
  -- FidoBiz Cross Sell
  when text like '%Upgrade for more cash! You already qualify for more with FidoBiz%' 
    or text like '%Upgrade to FidoBiz for more cash%' 
    or text like '%Your FidoBiz upgrade gets you more cash%' 
    or text like '%Get extra. Step things up and get up to GHS 8,200 with FidoBiz%' 
    or text like '%FidoBiz Cross Sell%' then 'FidoBiz Cross Sell'
  
  -- FidoBiz Non-eligible to Eligible
  when text like '%Take this chance! Get back in with a valid document%' 
    or (text like '%Upload a valid document (like MoMo statement) for up to GHS 8,200%' 
        and text like '%FidoBiz%') then 'FidoBiz Non-Eligible to Eligible'
  
  else null
end as broadcast_name,

case 
  -- Savings campaigns with target details (only for detailed Savings campaigns with "EasySave grows" pattern)
  when (text like '%EasySave%' 
    or text like '%b.fido.money/save%' 
    or text like '%10% interest%' 
    or text like '%Sign up for EasySave%' 
    or text like '%Sign up for FIDO''s EasySave%' 
    or text like '%Sign up for Fido''s EasySave%' 
    or text like '%Park your funds in EasySave%' 
    or text like '%EasySave grows%') then
    case
      -- Industry/Manufacturing: "grows your production funds" + GHS 150
      when text like '%EasySave grows your production funds%' 
        or (text like '%EasySave grows%' and text like '%Deposit GHS 150%') then 'Industry/Manufacturing, Closed, Age 25 - 34, Greater Accra'
      
      -- Agriculture: "grows your daily sales" + GHS 30 (but need to distinguish from Trade Retail)
      when text like '%EasySave grows your daily sales%' 
        and text like '%Deposit GHS 30%' 
        and not text like '%shop profits%' 
        and not text like '%or shop profits%' then 'Agriculture, Closed, Age 25 - 34, Greater Accra'
      
      -- Trade Retail: "grows your daily sales or shop profits" + GHS 30
      when text like '%EasySave grows your daily sales or shop profits%' 
        or (text like '%EasySave grows%' and text like '%shop profits%' and text like '%Deposit GHS 30%') then 'Trade Retail, Closed, Age 25 - 34, Greater Accra'
      
      -- Construction: "grows your project cash or job funds" + GHS 200
      when text like '%EasySave grows your project cash or job funds%' 
        or (text like '%EasySave grows%' and text like '%Deposit GHS 200%') then 'Construction, Closed, Age 25 - 34, Greater Accra'
      
      -- Healthcare: "grows your shift income or emergency fund" + GHS 100
      when text like '%EasySave grows your shift income or emergency fund%' 
        or (text like '%EasySave grows%' and text like '%shift income%' and text like '%Deposit GHS 100%') then 'Healthcare, Closed, Age 25 - 34, Greater Accra'
      
      -- Beauty: "grows your client earnings daily" + GHS 20 (check this before Domestic Services)
      when text like '%EasySave grows your client earnings daily%' 
        or (text like '%EasySave grows%' and text like '%client earnings daily%' and text like '%Deposit GHS 20%') then 'Beauty, Closed, Age 25 - 34, Greater Accra'
      
      -- Domestic Services: "grows your client earnings" + GHS 20 (but need to distinguish from Beauty)
      when text like '%EasySave grows your client earnings%' 
        and text like '%Deposit GHS 20%' 
        and not text like '%client earnings daily%' then 'Domestic Services, Closed, Age 25 - 34, Greater Accra'
      
      -- Education: "grows your salary or fees" + GHS 50
      when text like '%EasySave grows your salary or fees%' 
        or (text like '%EasySave grows%' and text like '%salary or fees%' and text like '%Deposit GHS 50%') then 'Education, Closed, Age 25 - 34, Greater Accra'
      
      -- All Industries: "grows your income" + GHS 50 (this is the default for income-based messages)
      when text like '%EasySave grows your income%' 
        and text like '%Deposit GHS 50%' then 'All Industries, Active/Active-in-arrears, Age 25 - 34, Greater Accra'
      
      -- Default for other Savings campaigns (simple ones without detailed targeting)
      else null
    end
  
  else null
end as target

from data.infobip_sms 