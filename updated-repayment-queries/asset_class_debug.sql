-- Debugging query to identify the issue with asset classification filtering

-- First, let's check what asset classifications exist in the CTE
WITH custom_field_keys AS (
    SELECT id, encodedkey
    FROM mambu.customfield
    WHERE id IN ('marital_status', 'location', 'home_owner', 'employment',
                 'employer_name', 'occupation', 'position', 'total_monthly_income')
),
base_loans AS (
    SELECT *
    FROM ml.loan_info_tbl
    WHERE accountstate IN ('ACTIVE', 'ACTIVE_IN_ARREARS', 'CLOSED_WRITTEN_OFF', 'CLOSED')
      AND date(lastmodifieddate) > date('2025-06-01') --and date('2025-06-30')
),
account_status_calc AS (
    SELECT
        loan_key,
        accountstate,
        CASE
            WHEN accountstate = 'ACTIVE' THEN 'A'
            WHEN accountstate = 'CLOSED_WRITTEN_OFF' THEN 'E'
            WHEN accountstate = 'ACTIVE_IN_ARREARS' THEN 'C'
            ELSE ''
        END as asset_classification,
        CASE
            WHEN accountstate = 'CLOSED' THEN 'C'
            WHEN accountstate = 'CLOSED_WRITTEN_OFF' THEN 'W'
            ELSE 'A'
        END as facility_status_code,
        IFF(accountstate = 'ACTIVE_IN_ARREARS', current_balance, 0) as amount_in_arrears,
        IFF(accountstate = 'ACTIVE_IN_ARREARS',
            DATEDIFF('day', lastSetToArrearsDate, CURRENT_DATE()),
            NULL) as ndia
    FROM base_loans
)

-- Debug Query 1: Check what asset classifications exist
SELECT 
    'Asset Classification Distribution' as debug_info,
    asset_classification,
    accountstate,
    COUNT(*) as count
FROM account_status_calc
GROUP BY asset_classification, accountstate
ORDER BY asset_classification, accountstate;

-- Debug Query 2: Check if JOIN is working properly
SELECT 
    'JOIN Test' as debug_info,
    ml.loan_id,
    ml.accountstate,
    asc.asset_classification,
    asc.accountstate as asc_accountstate
FROM base_loans ml
LEFT JOIN account_status_calc asc ON ml.loan_key = asc.loan_key
WHERE ml.loan_id IN (
    SELECT loan_id FROM base_loans LIMIT 5
)
ORDER BY ml.loan_id;

-- Debug Query 3: Check the actual filter issue
SELECT 
    'Filter Test' as debug_info,
    ml.loan_id,
    ml.accountstate,
    asc.asset_classification,
    CASE 
        WHEN asc.asset_classification IN ('A', 'C') THEN 'MATCH'
        ELSE 'NO MATCH'
    END as filter_result
FROM base_loans ml
LEFT JOIN account_status_calc asc ON ml.loan_key = asc.loan_key
WHERE asc.asset_classification IN ('A', 'C')
ORDER BY ml.loan_id
LIMIT 10; 