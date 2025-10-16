WITH custom_field_keys AS (
    -- Pre-fetch custom field keys to eliminate repeated subqueries
    SELECT id, encodedkey
    FROM mambu.customfield
    WHERE id IN ('marital_status', 'location', 'home_owner', 'employment',
                 'employer_name', 'occupation', 'position', 'total_monthly_income')
),
base_loans AS (
    -- Filter loans early to reduce dataset size
    SELECT *
    FROM ml.loan_info_tbl
    WHERE accountstate IN ('ACTIVE', 'ACTIVE_IN_ARREARS', 'CLOSED_WRITTEN_OFF', 'CLOSED')
      --AND creationdate < DATE_TRUNC('month', CURRENT_DATE())
),
client_identifications AS (
    -- Consolidate all identification documents into single subquery
    SELECT
        clientkey,
        MAX(CASE WHEN documenttype = 'Voter ID' THEN documentid END) as voter_id,
        MAX(CASE WHEN documenttype IN ('National ID', 'Ghana Card ID') THEN documentid END) as nat_id,
        MAX(CASE WHEN documenttype = 'Driver''s License' THEN documentid END) as driver_id,
        MAX(CASE WHEN documenttype = 'Passport' THEN documentid END) as passport_id,
        MAX(CASE WHEN documenttype = 'NHIA Membership' THEN documentid END) as nhia_id
    FROM mambu.identificationdocument
    WHERE documenttype IN ('Voter ID', 'National ID', 'Ghana Card ID', 'Driver''s License', 'Passport', 'NHIA Membership')
    GROUP BY clientkey
),
custom_field_values AS (
    -- Consolidate all custom field values into single query with pivot
    SELECT
        cfv.PARENTKEY,
        MAX(CASE WHEN cf.id = 'marital_status' THEN cfv.value END) as marital_status_value,
        MAX(CASE WHEN cf.id = 'location' THEN cfv.value END) as location_value,
        MAX(CASE WHEN cf.id = 'home_owner' THEN cfv.value END) as home_owner_value,
        MAX(CASE WHEN cf.id = 'employment' THEN cfv.value END) as employment_value,
        MAX(CASE WHEN cf.id = 'employer_name' THEN cfv.value END) as employer_name_value,
        MAX(CASE WHEN cf.id = 'occupation' THEN cfv.value END) as occupation_value,
        MAX(CASE WHEN cf.id = 'position' THEN cfv.value END) as position_value,
        MAX(CASE WHEN cf.id = 'total_monthly_income' THEN cfv.value END) as income_value
    FROM mambu.customfieldvalue cfv
    INNER JOIN custom_field_keys cf ON cfv.CUSTOMFIELDKEY = cf.encodedkey
    GROUP BY cfv.PARENTKEY
),
employment_classifications AS (
    -- Pre-calculate employment type mappings
    SELECT
        client_key,
        CASE
            WHEN custom_fields:employment = 'Salaried' THEN 101
            WHEN custom_fields:employment = 'Full-time Student' THEN 103
            WHEN custom_fields:employment = 'Contracted' THEN 104
            WHEN custom_fields:employment = 'Self-Employed' THEN 104
            WHEN custom_fields:employment = '' THEN 102
            ELSE 104
        END as emp_type
    FROM mambu.client_extra_values
),
income_classifications AS (
    -- Pre-calculate income mappings
    SELECT
        PARENTKEY,
        CASE
            WHEN income_value IS NULL THEN 'Not available'
            WHEN income_value = 'Below 350 GHS' THEN '350'
            WHEN income_value = '351 GHS - 700 GHS' THEN '700'
            WHEN income_value = '701 GHS - 1000 GHS' THEN '1000'
            WHEN income_value = '1001 GHS - 1400 GHS' THEN '1400'
            WHEN income_value = '1401 GHS - 1800 GHS' THEN '1800'
            WHEN income_value = 'Above 1800 GHS' THEN '2000'
            ELSE NULL
        END as income_mapped
    FROM custom_field_values
),
account_status_calc AS (
    -- Pre-calculate account status fields
    SELECT
        loan_key,
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
SELECT
    'D' AS "Data",
    '0' AS "CorrectionIndicator",
    ml.loan_id AS "FacilityAccNum",
    ml.lastmodifieddate,
    c.id AS "CustomerID",
    '1' AS "BranchCode",
    ci.nat_id as "NatIdNum",
    ci.voter_id as "VotersIDNum",
    ci.driver_id as "DriverLicNum",
    ci.passport_id as "PassportNum",
    NULL as "SSNum",
    NULL as "EzwichNum",
    IFF(ci.nhia_id IS NOT NULL, 'NHIS', NULL) as "OtherID",
    ci.nhia_id as "OtherIDNum",
    NULL as "TINum",
    LEFT(c.gender, 1) as "Gender",
    LEFT(cfv.marital_status_value, 1) as "MaritalStatus",
    'GHA' as "Nationality",
    TO_CHAR(c.birthdate,'YYYYMMDD') as "DOB",
    '' as "Title",
    c.lastName as "Surname",
    c.firstName as "FirstName",
    c.middleName as "MiddleNames",
    '' as "PrevName",
    '' as "ALIAS",
    '' as "ProofOfAddType",
    '' as "ProofOfAddNum",
    REPLACE(a.line1,'|',',') as "CurResAddr1",
    cfv.location_value as "CurResAddr2",
    a."region" as "CurResAddr3",
    '' as "CurResAddr4",
    '' as "CurResAddrPostalCode",
    '' as "DateMovedCurrRes",
    '' as "PrevResAddr1",
    '' as "PrevResAddr2",
    '' as "PrevResAddr3",
    '' as "PrevResAddr4",
    '' as "PrevResAddrPostalCode",
    CASE
        WHEN cfv.home_owner_value = 'You' THEN 'O'
        WHEN cfv.home_owner_value = 'Family' THEN 'F'
        WHEN cfv.home_owner_value IS NULL THEN ''
        ELSE 'T'
    END as "OwnerOrTenant",
    REPLACE(a.line2,'|',',') as "PostAddrLine1",
    '' as "PostAddrLine2",
    '' as "PostAddrLine3",
    '' as "PostAddrLine4",
    '' as "PostalAddPostCode",
    '' as "EmailAddress",
    '' as "HomeTel",
    c.mobilePhone1::text as "MobileTel1",
    '' as "MobileTel2",
    '' as "WorkTel",
    '' as "NumOfDependants",
    ec.emp_type as "EmpType",
    '' as "EmpPayrollNum",
    '' as "Paypoint",
    cfv.employer_name_value as "EmpName",
    '' as "EmpAddr1",
    '' as "EmpAddr2",
    '' as "EmpAddr3",
    '' as "EmpAddr4",
    '' as "EmpAddrPostalCode",
    '' as "DateOfEmp",
    COALESCE(cfv.occupation_value, cfv.position_value, 'Not available') as "occupation",
    ic.income_mapped as "income",
    'GHS' as "IncomeCurrency",
    'S' as "JointOrSoleAcc",
    '1' as "NoParticipantsInAcc",
    '' as "OldCustomerID",
    '' as "OldAccountNum",
    '' as "OldSRN",
    '' as "OldBranchCode",
    '119' as "CreditFacilityType",
    'P' as "PurposeOfFacility",
    DATEDIFF('day', ml.disbursementdate, ml.last_expected_repayment) as "FacilityTerm",
    '' as "DefPaymentStartDate",
    'GHS' as "AmountCurrency",
    ml.loanamount as "FacilityAmount",
    TO_CHAR(ml.disbursementdate,'YYYYMMDD') as "DisbursementDate",
    ml.loanamount as "DisbursementAmt",
    TO_CHAR(ml.last_expected_repayment,'YYYYMMDD') as "MaturityDate",
    ROUND(nr.schd_instal_amount) as "SchdInstalAmount",
    IFF(ml.repaymentinstallments > 1, '12', '18') as "RepaymentFreq",
    CAST(ROUND(ml.total_repayment_amount) AS INT) AS "LastPaymentAmount",
    TO_CHAR(ml.last_repayment_date,'YYYYMMDD') as "LastPaymentDate",
    TO_CHAR(r.next_due_repayment, 'YYYYMMDD') as "NextPaymentDate",
    ml.current_balance as "CurBal",
    'D' as "CurBalIndicator",
    asc.asset_classification as "AssetClassification",
    asc.amount_in_arrears as "AmountInArrears",
    TO_CHAR(ml.lastSetToArrearsDate,'YYYYMMDD') as "ArrearsStartDate",
    asc.ndia as "NDIA",
    '' as "PaymentHistoryProfile",
    CAST(ROUND(r.amt_overdue31_60d) AS INT) AS "AmtOverdue31to60days",
    CAST(ROUND(r.amt_overdue61_90d) AS INT) AS "AmtOverdue61to90days",
    CAST(ROUND(r.amt_overdue91_120d) AS INT) AS "AmtOverdue91to120days",
    CAST(ROUND(r.amt_overdue121_150d) AS INT) AS "AmtOverdue121to150days",
    CAST(ROUND(r.amt_overdue151_180d) AS INT) AS "AmtOverdue151to180days",
    CAST(ROUND(r.amt_overdue181_d) AS INT) AS "AmtOverdue181orMore",
    '101' as "LegalFlag",
    asc.facility_status_code as "FacilityStatusCode",
    TO_CHAR(CURRENT_DATE,'YYYYMMDD') as "FacilityStatusDate",
    TO_CHAR(IFF(ml.accountstate IN ('CLOSED', 'CLOSED_WRITTEN_OFF'), ml.closeddate, NULL), 'YYYYMMDD') as "ClosedDate",
    CASE WHEN ml.loan_repaid_date < ml.last_expected_repayment THEN 'F' ELSE '' END as "ClosureReason",
    lt.WRITTEN_OFF_AMOUNT as "WrittenOffAmt",
    CASE
        WHEN lt.WRITTEN_OFF_AMOUNT IS NOT NULL AND lt.total_repayment_amount > 0 THEN 'F'
        WHEN lt.WRITTEN_OFF_AMOUNT IS NOT NULL THEN 'F'
        ELSE ''
    END as "ReasonForWrittenOff",
    '' as "DateRestructured",
    '' as "ReasonForRestructure",
    '102' as "CreditCollateralInd",
    '' as "SecurityType",
    '' as "NatureOfCharge",
    '' as "SecurityValue",
    '' as "CollRegRefNum",
    '' as "SpecialCommentsCode",
    '103' as "NatureofGuarantor",
    '' as "NameofComGuarantor",
    '' as "BusRegOfGuarantor",
    '' as "G1Surname",
    '' as "G1FirstName",
    '' as "G1MiddleNames",
    '' as "G1NatID",
    '' as "G1VotID",
    '' as "G1DrivLic",
    '' as "G1PassNum",
    '' as "G1SSN",
    '' as "G1Gender",
    '' as "G1DOB",
    '' as "G1Add1",
    '' as "G1Add2",
    '' as "G1Add3",
    '' as "G1HomeTel",
    '' as "G1WorkTel",
    '' as "G1Mobile",
    '' as "G2Surname",
    '' as "G2FirstName",
    '' as "G2MiddleNames",
    '' as "G2NatID",
    '' as "G2VotID",
    '' as "G2DrivLic",
    '' as "G2PassNum",
    '' as "G2SSN",
    '' as "G2Gender",
    '' as "G2DOB",
    '' as "G2Add1",
    '' as "G2Add2",
    '' as "G2Add3",
    '' as "G2HomeTel",
    '' as "G2WorkTel",
    '' as "G2Mobile",
    '' as "G3Surname",
    '' as "G3FirstName",
    '' as "G3MiddleNames",
    '' as "G3NatID",
    '' as "G3VotID",
    '' as "G3DrivLic",
    '' as "G3PassNum",
    '' as "G3SSN",
    '' as "G3Gender",
    '' as "G3DOB",
    '' as "G3Add1",
    '' as "G3Add2",
    '' as "G3Add3",
    '' as "G3HomeTel",
    '' as "G3WorkTel",
    '' as "G3Mobile",
    '' as "G4Surname",
    '' as "G4FirstName",
    '' as "G4MiddleNames",
    '' as "G4NatID",
    '' as "G4VotID",
    '' as "G4DrivLic",
    '' as "G4PassNum",
    '' as "G4SSN",
    '' as "G4Gender",
    '' as "G4DOB",
    '' as "G4Add1",
    '' as "G4Add2",
    '' as "G4Add3",
    '' as "G4HomeTel",
    '' as "G4WorkTel",
    '' as "G4Mobile"
FROM base_loans ml
INNER JOIN mambu.client c ON ml.client_key = c.encodedkey
INNER JOIN ml.repayment r ON ml.loan_key = r.loan_key
LEFT JOIN ml.next_repayment nr ON ml.loan_key = nr.loan_key
INNER JOIN ml.LOAN_TRANSACTION_SUMMARY lt ON ml.loan_key = lt.loan_key
LEFT JOIN client_identifications ci ON c.encodedkey = ci.clientkey
LEFT JOIN mambu.address a ON c.ENCODEDKEY = a.PARENTKEY
LEFT JOIN custom_field_values cfv ON c.ENCODEDKEY = cfv.PARENTKEY
LEFT JOIN employment_classifications ec ON c.encodedkey = ec.client_key
LEFT JOIN income_classifications ic ON c.ENCODEDKEY = ic.PARENTKEY
LEFT JOIN account_status_calc asc ON ml.loan_key = asc.loan_key
where date(ml.lastmodifieddate) between date('2025-06-01') and date('2025-06-30')
and asc.asset_classification ='A'
order by "FacilityAccNum"