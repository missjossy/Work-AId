-- Auto-assign SMS Campaign Goals based on User Pipeline Step and Text Keywords
-- This query determines the goal of each SMS campaign for each user by:
-- 1. Identifying the last completed step in the user journey pipeline (based on fido_credit&savers_pipeline.sql)
-- 2. Using keywords in the SMS text to refine the goal assignment
--
-- IMPORTANT: Adjust column names in the sms_data CTE based on your actual DATA.INFOBIP_SMS table structure
-- Common column names might be:
--   - phone_number, phone, recipient, destination_address, etc.
--   - sent_at, sent_date, timestamp, created_at, etc.
--   - text, message, message_text, content, etc.

WITH sms_data AS (
    SELECT 
        sms.*,
        -- Extract user/client identifier from SMS
        -- TODO: Adjust these column names to match your actual DATA.INFOBIP_SMS table structure
        COALESCE(
            sms.phone_number,
            sms.phone,
            sms.recipient,
            sms.destination_address,
            sms.to_number
        ) as phone_number,  -- Adjust based on actual column name
        
        COALESCE(
            sms.sent_at,
            sms.sent_date,
            sms.timestamp,
            sms.created_at,
            sms.sent_timestamp
        ) as sent_at,       -- Adjust based on actual column name
        
        COALESCE(
            sms.text,
            sms.message,
            sms.message_text,
            sms.content,
            sms.body
        ) as text           -- Adjust based on actual column name
    FROM DATA.INFOBIP_SMS sms
),

-- Map SMS to users via phone number
sms_with_users AS (
    SELECT 
        s.*,
        u.ID as user_id,
        u.BANKING_PLATFORM_ID as client_id,
        u.CREATED_TIMESTAMP as signup_date
    FROM sms_data s
    LEFT JOIN GHANA_PROD.BANKING_SERVICE."USER" u 
        ON REPLACE(s.phone_number, ' ', '') = REPLACE(u.PHONE_NUMBER, ' ', '')
),

-- Determine user's last completed step in pipeline before SMS was sent
user_pipeline_steps AS (
    SELECT 
        swu.*,
        -- Sign up step
        CASE WHEN swu.signup_date IS NOT NULL 
             AND swu.signup_date <= swu.sent_at 
             THEN swu.signup_date ELSE NULL END as signup_completed_at,
        
        -- KYC verified step
        kyc.timestamp as kyc_verified_at,
        
        -- Survey filled step
        survey.loan_date as survey_filled_at,
        
        -- FS eligible step (from fido_score table)
        fs.created_on as fs_eligible_at,
        
        -- FS eligible step (from client_info table)
        ci.survey_date as fs_eligible_ci_at,
        
        -- Loan attempt step
        loan_attempt.creationdate as loan_attempt_at,
        
        -- Loan disbursement step
        loan_disb.disbursementdate as loan_disbursed_at,
        
        -- Savings account creation step
        savings.creationdate as savings_created_at,
        
        -- FidoBiz KYB submitted
        kyb.timestamp as kyb_submitted_at,
        
        -- FidoBiz document status
        biz_doc.timestamp as biz_doc_at
        
    FROM sms_with_users swu
    
    -- KYC verified
    LEFT JOIN (
        SELECT DISTINCT 
            USER_IDENTITY,
            MIN(timestamp) as timestamp
        FROM GHANA_PROD.DATA.BACKEND_NOTIFICATIONS 
        WHERE TYPE = 'BE_KYC_VERIFICATION_RESULT'
            AND parse_json(payload):status = 'succeeded'
            AND parse_json(payload):is_duplicate = false
        GROUP BY USER_IDENTITY
    ) kyc ON kyc.USER_IDENTITY = swu.user_id 
        AND kyc.timestamp <= swu.sent_at
    
    -- Survey filled
    LEFT JOIN (
        SELECT DISTINCT 
            client_id,
            MAX(LOAN_DATE) as loan_date
        FROM GHANA_PROD.DATA.SURVEY_DATA
        GROUP BY client_id
    ) survey ON survey.client_id = swu.client_id
        AND survey.loan_date <= swu.sent_at
    
    -- FS eligible (from fido_score table)
    LEFT JOIN (
        SELECT DISTINCT 
            client_id,
            MIN(created_on) as created_on
        FROM GHANA_PROD.DATA.FIDO_SCORE
        WHERE score >= 250
        GROUP BY client_id
    ) fs ON fs.client_id = swu.client_id
        AND fs.created_on <= swu.sent_at
    
    -- FS eligible (from client_info table)
    LEFT JOIN (
        SELECT DISTINCT 
            client_id,
            survey_date
        FROM GHANA_PROD.ML.CLIENT_INFO
        WHERE first_fido_score >= 250 
            AND (FRAUD_TYPE_MATCHED = '' OR FRAUD_TYPE_MATCHED IS NULL)
            AND (PERSONAL_BR_DECISION IS NULL OR PERSONAL_BR_DECISION = 'APPROVED')
    ) ci ON ci.client_id = swu.client_id
        AND ci.survey_date <= swu.sent_at
    
    -- Loan attempt
    LEFT JOIN (
        SELECT DISTINCT 
            CLIENT_KEY,
            CLIENT_ID,
            MIN(CREATIONDATE) as creationdate
        FROM GHANA_PROD.ML.LOAN_INFO_TBL
        GROUP BY CLIENT_KEY, CLIENT_ID
    ) loan_attempt ON loan_attempt.CLIENT_ID = swu.client_id
        AND loan_attempt.creationdate <= swu.sent_at
    
    -- Loan disbursement
    LEFT JOIN (
        SELECT DISTINCT 
            CLIENT_KEY,
            CLIENT_ID,
            MIN(DISBURSEMENTDATE) as disbursementdate,
            MAX(LN) as max_ln
        FROM GHANA_PROD.ML.LOAN_INFO_TBL
        WHERE DISBURSEMENTDATE IS NOT NULL
        GROUP BY CLIENT_KEY, CLIENT_ID
    ) loan_disb ON loan_disb.CLIENT_ID = swu.client_id
        AND loan_disb.disbursementdate <= swu.sent_at
    
    -- Savings account creation
    LEFT JOIN (
        SELECT DISTINCT 
            cl.id as client_id,
            MIN(sa.creationdate) as creationdate
        FROM GHANA_PROD.MAMBU.SAVINGSACCOUNT sa
        LEFT JOIN MAMBU.CLIENT cl ON cl.ENCODEDKEY = sa.ACCOUNTHOLDERKEY
        WHERE sa.ACCOUNTTYPE = 'REGULAR_SAVINGS'
            AND sa.accountstate != 'WITHDRAWN'
        GROUP BY cl.id
    ) savings ON savings.client_id = swu.client_id
        AND savings.creationdate <= swu.sent_at
    
    -- FidoBiz KYB submitted
    LEFT JOIN (
        SELECT DISTINCT 
            USER_IDENTITY,
            MIN(timestamp) as timestamp
        FROM GHANA_PROD.DATA.BACKEND_NOTIFICATIONS
        WHERE TYPE = 'BE_FIDOBIZ_SURVEY_SUBMISSION'
        GROUP BY USER_IDENTITY
    ) kyb ON kyb.USER_IDENTITY = swu.user_id
        AND kyb.timestamp <= swu.sent_at
    
    -- FidoBiz document status
    LEFT JOIN (
        SELECT DISTINCT 
            USER_IDENTITY,
            MIN(timestamp) as timestamp
        FROM GHANA_PROD.DATA.BACKEND_NOTIFICATIONS
        WHERE TYPE = 'BE_FIDOBIZ_SCORE_DOCUMENT_STATUS_EVENT'
            AND parse_json(payload):updated_status::string = 'IN_REVIEW'
        GROUP BY USER_IDENTITY
    ) biz_doc ON biz_doc.USER_IDENTITY = swu.user_id
        AND biz_doc.timestamp <= swu.sent_at
),

-- Determine the last completed step for each user at SMS time
user_last_step AS (
    SELECT 
        *,
        -- Determine the last completed step (in order of pipeline)
        CASE 
            WHEN loan_disbursed_at IS NOT NULL THEN 'disbursed'
            WHEN loan_attempt_at IS NOT NULL THEN 'loan_attempt'
            WHEN fs_eligible_at IS NOT NULL OR fs_eligible_ci_at IS NOT NULL THEN 'fs_eligible'
            WHEN survey_filled_at IS NOT NULL THEN 'survey_filled'
            WHEN kyc_verified_at IS NOT NULL THEN 'kyc_verified'
            WHEN signup_completed_at IS NOT NULL THEN 'signed_up'
            ELSE 'none'
        END as last_completed_step,
        
        -- Additional context for FidoBiz
        CASE 
            WHEN biz_doc_at IS NOT NULL THEN 'biz_doc_submitted'
            WHEN kyb_submitted_at IS NOT NULL THEN 'kyb_submitted'
            ELSE NULL
        END as fidobiz_status,
        
        -- Check if user has savings
        CASE WHEN savings_created_at IS NOT NULL THEN TRUE ELSE FALSE END as has_savings,
        
        -- Check if user has loans
        CASE WHEN loan_disbursed_at IS NOT NULL THEN TRUE ELSE FALSE END as has_loans,
        
        -- Get max loan number if user has loans
        (SELECT MAX(LN) 
         FROM GHANA_PROD.ML.LOAN_INFO_TBL 
         WHERE CLIENT_ID = ups.client_id 
           AND DISBURSEMENTDATE IS NOT NULL
           AND DISBURSEMENTDATE <= ups.sent_at) as max_ln
        
    FROM user_pipeline_steps ups
),

-- Identify keywords in SMS text
keyword_detection AS (
    SELECT 
        *,
        -- Detect campaign type from text keywords
        CASE 
            -- FidoBiz KYB
            WHEN UPPER(text) LIKE '%FIDOBIZ%' 
                AND (UPPER(text) LIKE '%UPLOAD%' OR UPPER(text) LIKE '%DOCUMENT%') 
                THEN 'FidoBiz KYB'
            
            -- KYC
            WHEN (UPPER(text) LIKE '%FINISH YOUR SIGNUP%' 
                  OR UPPER(text) LIKE '%VERIFY YOUR ID%')
                AND UPPER(text) NOT LIKE '%*998*88#%'
                THEN 'KYC'
            
            -- KYC USSD
            WHEN UPPER(text) LIKE '%*998*88#%' 
                OR UPPER(text) LIKE '%DIAL *998*88#%'
                THEN 'KYC USSD'
            
            -- Non-eligible to Eligible
            WHEN UPPER(text) LIKE '%MOMO STATEMENT%' 
                AND UPPER(text) LIKE '%LOAN APPROVAL%'
                AND UPPER(text) NOT LIKE '%FIDOBIZ%'
                THEN 'Non-eligible to Eligible'
            
            -- Savings
            WHEN (UPPER(text) LIKE '%EASYSAVE%' 
                  OR UPPER(text) LIKE '%B.FIDO.MONEY/SAVE%'
                  OR UPPER(text) LIKE '%10% INTEREST%'
                  OR UPPER(text) LIKE '%SIGN UP FOR EASYSAVE%'
                  OR UPPER(text) LIKE '%PARK YOUR FUNDS%'
                  OR UPPER(text) LIKE '%EASYSAVE GROWS%')
                THEN 'Savings'
            
            -- USSD Dropoff
            WHEN UPPER(text) LIKE '%FIDO OFFERS UP TO GHS 8,200 IN A FEW SIMPLE STEPS%'
                THEN 'USSD Dropoff'
            
            -- Referrals (general)
            WHEN (UPPER(text) LIKE '%MAKE MONEY FROM REFERRALS%' 
                  OR UPPER(text) LIKE '%EARN GHS 55%'
                  OR UPPER(text) LIKE '%APP.FIDO.MONEY/INVITE-FRIENDS%')
                AND UPPER(text) NOT LIKE '%FIDOBIZ%'
                THEN 'Referrals'
            
            -- FidoBiz Referrals
            WHEN (UPPER(text) LIKE '%FIDOBIZ%' 
                  AND (UPPER(text) LIKE '%REFERRAL%' OR UPPER(text) LIKE '%REFERRALS%'))
                THEN 'FidoBiz Referrals'
            
            -- FidoBiz Approved
            WHEN UPPER(text) LIKE '%YOU''RE ALREADY APPROVED FOR AN INSTANT FIDOBIZ LOAN%'
                OR UPPER(text) LIKE '%YOU''RE ALREADY APPROVED FOR AN INSTANT FIDOBIZ%'
                THEN 'FidoBiz Approved'
            
            -- Eligible
            WHEN UPPER(text) LIKE '%YOU ARE ALREADY ELIGIBLE FOR A FIDO LOAN%'
                THEN 'Eligible'
            
            -- FidoBiz LPs
            WHEN UPPER(text) LIKE '%THOUSANDS OF PEOPLE ARE CASHING OUT THEIR FIDOBIZ LOAN%'
                OR UPPER(text) LIKE '%LANDING PAGE%'
                THEN 'FidoBiz LPs'
            
            -- FidoBiz Cross Sell
            WHEN (UPPER(text) LIKE '%UPGRADE%' 
                  OR UPPER(text) LIKE '%UPGRADE TO FIDOBIZ%'
                  OR UPPER(text) LIKE '%FIDOBIZ UPGRADE%')
                AND UPPER(text) LIKE '%FIDOBIZ%'
                THEN 'FidoBiz Cross Sell'
            
            -- FidoBiz Non-eligible to Eligible
            WHEN UPPER(text) LIKE '%FIDOBIZ%'
                AND (UPPER(text) LIKE '%UPLOAD A VALID DOCUMENT%' 
                     OR UPPER(text) LIKE '%GET BACK IN%')
                THEN 'FidoBiz Non-eligible to Eligible'
            
            ELSE NULL
        END as detected_campaign_type
        
    FROM user_last_step
),

-- Assign goals based on pipeline step + keywords + context
goal_assignment AS (
    SELECT 
        kd.*,
        
        -- Assign goal based on detected campaign type and user's pipeline position
        CASE 
            -- FidoBiz KYB: User should have submitted KYB but not submitted document
            WHEN detected_campaign_type = 'FidoBiz KYB' 
                AND fidobiz_status = 'kyb_submitted'
                AND biz_doc_at IS NULL
                THEN 'When the user does: BE_FIDOBIZ_SCORE_DOCUMENT_STATUS_EVENT updated_status ∋ APPROVED'
            
            -- KYC: User should have completed KYC but not survey
            WHEN detected_campaign_type = 'KYC'
                AND last_completed_step = 'kyc_verified'
                AND survey_filled_at IS NULL
                THEN 'When a user does BE_KYC_VERIFICATION_RESULT status = succeeded'
            
            -- KYC USSD: User should have completed KYC
            WHEN detected_campaign_type = 'KYC USSD'
                AND last_completed_step IN ('kyc_verified', 'signed_up')
                THEN 'When a user does BE_KYC_VERIFICATION_RESULT status = succeeded'
            
            -- Non-eligible to Eligible: User should have FS score but not eligible yet, or became eligible
            WHEN detected_campaign_type = 'Non-eligible to Eligible'
                AND (last_completed_step IN ('fs_eligible', 'survey_filled', 'kyc_verified')
                     OR (fs_eligible_at IS NULL AND fs_eligible_ci_at IS NULL))
                THEN 'When a user does BE_FIDOSCORE_SCORE_CALCULATED score > 249'
            
            -- Savings: User should have created savings account or be eligible
            WHEN detected_campaign_type = 'Savings'
                AND (has_savings = TRUE 
                     OR last_completed_step IN ('fs_eligible', 'survey_filled', 'kyc_verified', 'signed_up'))
                THEN 'When a user does ME - Savings event_description ∋ Account Created'
            
            -- USSD Dropoff: User should have signed up but not completed KYC or loan
            WHEN detected_campaign_type = 'USSD Dropoff'
                AND last_completed_step IN ('signed_up', 'kyc_verified')
                AND loan_disbursed_at IS NULL
                THEN 'When a user gets their first disbursement (L0)'
            
            -- Referrals: User should have at least one disbursement
            WHEN detected_campaign_type = 'Referrals'
                AND last_completed_step = 'disbursed'
                AND max_ln >= 3
                THEN 'ME - Referral event_description ∋ Referee Register Link Activated'
            
            -- FidoBiz Referrals: User should have FidoBiz disbursement
            WHEN detected_campaign_type = 'FidoBiz Referrals'
                AND last_completed_step = 'disbursed'
                THEN 'FidoBiz First Disbursement'
            
            -- FidoBiz Approved: User should have FidoBiz approval but no disbursement
            WHEN detected_campaign_type = 'FidoBiz Approved'
                AND fidobiz_status = 'biz_doc_submitted'
                AND loan_disbursed_at IS NULL
                THEN 'When a user does BE_LOAN_FIRST_DISBURSEMENT loan_product ∋ UCBLL'
            
            -- Eligible: User should be eligible but not disbursed
            WHEN detected_campaign_type = 'Eligible'
                AND last_completed_step IN ('fs_eligible', 'loan_attempt')
                AND loan_disbursed_at IS NULL
                THEN 'When a user does BE_LOAN_FIRST_DISBURSEMENT loan_product ∋ UCBLL'
            
            -- FidoBiz LPs: User should have visited landing page (no signup or early stage)
            WHEN detected_campaign_type = 'FidoBiz LPs'
                AND last_completed_step IN ('none', 'signed_up', 'kyc_verified')
                THEN 'FidoBiz First Disbursement'
            
            -- FidoBiz Cross Sell: User should have personal loans (L0-L3) but not FidoBiz
            WHEN detected_campaign_type = 'FidoBiz Cross Sell'
                AND last_completed_step = 'disbursed'
                AND max_ln BETWEEN 0 AND 3
                AND (SELECT COUNT(*) 
                     FROM GHANA_PROD.ML.LOAN_INFO_TBL 
                     WHERE CLIENT_ID = kd.client_id 
                       AND LOAN_PRODUCT_ID LIKE '%UCBLL%'
                       AND DISBURSEMENTDATE IS NOT NULL
                       AND DISBURSEMENTDATE <= kd.sent_at) = 0
                THEN 'When a user does BE_FIDOBIZ_SCORE_DOCUMENT_STATUS_EVENT updated_status ∋ APPROVED'
            
            -- FidoBiz Non-eligible to Eligible: User should have been rejected but became eligible
            WHEN detected_campaign_type = 'FidoBiz Non-eligible to Eligible'
                AND fidobiz_status IS NOT NULL
                THEN 'When a user does BE_FIDOBIZ_SCORE_DOCUMENT_STATUS_EVENT updated_status ∋ APPROVED'
            
            -- Default: Use detected campaign type if no specific match
            WHEN detected_campaign_type IS NOT NULL
                THEN 'Auto-assigned: ' || detected_campaign_type
            
            ELSE 'Unknown - Manual Review Required'
        END as assigned_goal
        
    FROM keyword_detection kd
)

-- Final output
-- Output columns:
--   phone_number: Phone number of the SMS recipient
--   user_id: User ID from BANKING_SERVICE.USER table
--   client_id: Client ID (BANKING_PLATFORM_ID) from USER table
--   sent_at: Timestamp when SMS was sent
--   text: SMS message text
--   last_completed_step: Last completed step in the user journey pipeline before SMS was sent
--                        Values: 'none', 'signed_up', 'kyc_verified', 'survey_filled', 
--                                'fs_eligible', 'loan_attempt', 'disbursed'
--   fidobiz_status: FidoBiz-specific status ('kyb_submitted', 'biz_doc_submitted', or NULL)
--   has_savings: Boolean indicating if user has a savings account
--   has_loans: Boolean indicating if user has disbursed loans
--   max_ln: Maximum loan number (LN) for the user at SMS time
--   detected_campaign_type: Campaign type detected from SMS text keywords
--   assigned_goal: Automatically assigned goal based on pipeline step + keywords
--                 This matches the "Goal" column format from sms_campaigns_and_goals.csv
SELECT 
    phone_number,
    user_id,
    client_id,
    sent_at,
    text,
    last_completed_step,
    fidobiz_status,
    has_savings,
    has_loans,
    max_ln,
    detected_campaign_type,
    assigned_goal
FROM goal_assignment
ORDER BY sent_at DESC, phone_number

