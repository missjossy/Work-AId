SELECT 'D' AS "Data",
       '0' AS "CorrectionIndicator",
       ml.loan_id AS "FacilityAccNum",
       c.id AS "CustomerID",
       '1' AS "BranchCode",
       i_nat_id.documentid AS "NatIdNum",
       i_voter_id.documentid AS "VotersIDNum",
       i_driver_id.documentid AS "DriverLicNum",
       i_passport_id.documentid AS "PassportNum",
       NULL AS "SSNum",
       NULL AS "EzwichNum",
       IFF(i_nhia_id.documentid IS NOT NULL, 'NHIS', NULL) AS "OtherID",
       i_nhia_id.documentid AS "OtherIDNum",
       NULL AS "TINum",
       LEFT(c.gender, 1) Gender,
       LEFT(marital_status.value, 1) AS "MaritalStatus",
       'GHA' AS "Nationality",
       TO_CHAR(c.birthdate,'YYYYMMDD') AS "DOB",
       '' AS "Title",
       c.lastName AS "Surname",
       c.firstName AS "FirstName",
       CASE 
           WHEN c.middleName IS NULL OR 
                LOWER(TRIM(c.middleName)) IN ('no name', 'noname', 'no', 'n/a', 'na', 'none', 'null', '') OR
                LOWER(TRIM(c.middleName)) LIKE '%no name%' OR
                LOWER(TRIM(c.middleName)) LIKE '%noname%'
           THEN ''
           ELSE c.middleName
       END AS "MiddleNames",
       '' AS "PrevName",
       '' AS "Alias",
       '' AS "ProofOfAddType",
       '' AS "ProofOfAddNum",
       REPLACE(a.line1,'|',',') AS "CurResAddr1",
       location.value AS "CurResAddr2",
       a."region" as "CurResAddr3",
       '' as "CurResAddr4",
 '' AS "CurResAddrPostalCode",
 '' AS "DateMovedCurrRes",
 '' AS "PrevResAddr1",
 '' AS "PrevResAddr2",
 '' AS "PrevResAddr3",
 '' AS "PrevResAddr4",
 '' AS "PrevResAddrPostalCode",
 CASE
     WHEN home_owner.value = 'You' THEN 'O'
     WHEN home_owner.value = 'Family' THEN 'F'
     WHEN home_owner.value IS NULL THEN ''
     ELSE 'T'
 END AS "OwnerOrTenant",
 REPLACE(a.line2,'|',',') AS "PostAddrLine1",
 '' AS "PostAddrLine2",
 '' AS "PostAddrLine3",
 '' AS "PostAddrLine4",
 '' AS "PostalAddPostCode",
 '' AS "EmailAddress",
 '' AS "HomeTel",
 c.mobilePhone1::text AS "MobileTel1",
 '' AS "MobileTel2",
 '' AS "WorkTel",
 '' AS "NumOfDependants",
 CASE
     WHEN ccf.custom_fields:employment = 'Salaried' THEN 101
     WHEN ccf.custom_fields:employment = 'Full-time Student' THEN 103
     WHEN ccf.custom_fields:employment = 'Contracted' THEN 104
     WHEN ccf.custom_fields:employment = 'Self-Employed' THEN 104
     WHEN ccf.custom_fields:employment = '' THEN 102
     ELSE 104
 END AS "EmpType",
 '' AS "EmpPayrollNum",
 '' AS "Paypoint",
 employer_name.value AS "EmpName",
 '' AS "EmpAddr1",
 '' AS "EmpAddr2",
 '' AS "EmpAddr3",
 '' AS "EmpAddr4",
 '' AS "EmpAddrPostalCode",
 '' AS "DateOfEmp",
 COALESCE(occupation.value, position.value, 'Not available') AS "Occupation",
 'GHS' AS "IncomeCurrency",
 CASE
     WHEN income.value IS NULL THEN 'Not available'
     WHEN income.value = 'Below 350 GHS' THEN '350'
     WHEN income.value = '351 GHS - 700 GHS' THEN '700'
     WHEN income.value = '701 GHS - 1000 GHS' THEN '1000'
     WHEN income.value = '1001 GHS - 1400 GHS' THEN '1400'
     WHEN income.value = '1401 GHS - 1800 GHS' THEN '1800'
     WHEN income.value = 'Above 1800 GHS' THEN '2000'
     ELSE NULL
 END AS "Income",
 'S' AS "JointOrSoleAcc",
 '1' AS "NoParticipantsInAcc",
 '' AS "OldCustomerID",
 '' AS "OldAccountNum",
 '' AS "OldSRN",
 '' AS "OldBranchCode",
 '119' AS "CreditFacilityType",
 'P' AS "PurposeOfFacility",
 datediff('day', ml.disbursementdate, ml.last_expected_repayment) AS "FacilityTerm",
 '' AS "DefPaymentStartDate",
 'GHS' AS "AmountCurrency",
 ml.loanamount AS "FacilityAmount",
 TO_CHAR(ml.disbursementdate,'YYYYMMDD') AS "DisbursementDate",
 ml.loanamount AS "DisbursementAmt",
 TO_CHAR(ml.last_expected_repayment,'YYYYMMDD') AS "MaturityDate",
 ROUND(nr.schd_instal_amount) AS "SchdInstalAmount",
 IFF(ml.repaymentinstallments > 1, '12', '18') AS "RepaymentFreq",
 CASE
     WHEN abs(revised_writeoff_amount) < 10 THEN CAST(ROUND(recovered.lastpaidamount) AS INT)
     ELSE CAST(ROUND(ml.total_repayment_amount) AS INT)
 END AS "LastPaymentAmount",
 CASE
     WHEN abs(revised_writeoff_amount) < 10 THEN TO_CHAR(recovered.lastpaiddate,'YYYYMMDD')
     ELSE TO_CHAR(ml.last_repayment_date,'YYYYMMDD')
 END AS "LastPaymentDate",
 TO_CHAR(r.next_due_repayment, 'YYYYMMDD') "NextPaymentDate",
 ml.current_balance AS "CurBal",
 'D' AS "CurBalIndicator",
 CASE
     WHEN (ml.accountstate = 'CLOSED_WRITTEN_OFF'
           AND abs(revised_writeoff_amount) > 10) THEN 'E'
     WHEN ml.accountstate = 'ACTIVE' THEN 'A'
     WHEN ml.accountstate = 'ACTIVE_IN_ARREARS' THEN 'C'
     ELSE ''
 END AS "AssetClassification",
 IFF(ml.accountState = 'ACTIVE_IN_ARREARS', ml.current_balance, 0) AS "AmountInArrears",
 TO_CHAR(ml.lastSetToArrearsDate,'YYYYMMDD') AS "ArrearsStartDate",
 IFF(ml.accountState = 'ACTIVE_IN_ARREARS', DATEDIFF('day', ml.lastSetToArrearsDate,CURRENT_DATE()), NULL) AS "NDIA",
 '' AS "PaymentHistoryProfile",
 CAST(ROUND(r.amt_overdue1_30d) AS INT) AS "AmtOverdue1to30days",
 CAST(ROUND(r.amt_overdue31_60d) AS INT) AS "AmtOverdue31to60days",
 CAST(ROUND(r.amt_overdue61_90d) AS INT) AS "AmtOverdue61to90days",
 CAST(ROUND(r.amt_overdue91_120d) AS INT) AS "AmtOverdue91to120days",
 CAST(ROUND(r.amt_overdue121_150d) AS INT) AS "AmtOverdue121to150days",
 CAST(ROUND(r.amt_overdue151_180d) AS INT) AS "AmtOverdue151to180days",
 CAST(ROUND(r.amt_overdue181_d) AS INT) AS "AmtOverdue181orMore",
 '101' AS "LegalFlag",
 CASE
     WHEN ml.accountState = 'CLOSED' THEN 'C'
     WHEN (ml.accountState = 'CLOSED_WRITTEN_OFF'
           AND abs(recovered.revised_writeoff_amount) < 10) THEN 'C'
     WHEN (ml.accountState = 'CLOSED_WRITTEN_OFF'
           AND abs(recovered.revised_writeoff_amount) > 10) THEN 'W'
     WHEN (ml.accountState = 'CLOSED_WRITTEN_OFF'
           AND recovered.revised_writeoff_amount IS NULL) THEN 'W'
     ELSE 'A'
 END AS "FacilityStatusCode",
 TO_CHAR(CURRENT_DATE,'YYYYMMDD') AS "FacilityStatusDate",
 CASE
     WHEN abs(revised_writeoff_amount) < 10 THEN TO_CHAR(recovered.lastpaiddate,'YYYYMMDD')
     ELSE TO_CHAR(ml.closeddate,'YYYYMMDD')
 END AS "ClosedDate",
 CASE
     WHEN ml.loan_repaid_date < last_expected_repayment THEN 'F'
     ELSE ''
 END AS "ClosureReason",
 CASE
     WHEN abs(recovered.revised_writeoff_amount) > 10 THEN CAST(ROUND(recovered.revised_writeoff_amount) AS INT)
     ELSE 0
 END AS "WrittenOffAmt",
 CASE
     WHEN (lt.WRITTEN_OFF_AMOUNT IS NOT NULL
           AND lt.total_repayment_amount > 0
           AND abs(recovered.revised_writeoff_amount) > 10) THEN 'F' -- TODO: needs to be changed into A, after excluding corrections falsely marked as repayments

     WHEN (lt.WRITTEN_OFF_AMOUNT IS NOT NULL
           AND abs(recovered.revised_writeoff_amount) > 10) THEN 'F'
     ELSE ''
 END AS "ReasonForWrittenOff",
 '' AS "DateRestructured",
 '' AS "ReasonForRestructure",
 '102' AS "CreditCollateralInd",
 '' AS "SecurityType",
 '' AS "NatureOfCharge",
 '' AS "SecurityValue",
 '' AS "CollRegRefNum",
 '' AS "SpecialCommentsCode",
 '103' AS "NatureofGuarantor",
 '' AS "NameofComGuarantor",
 '' AS "BusRegOfGuarantor",
 '' AS "G1Surname",
 '' AS "G1FirstName",
 '' AS "G1MiddleNames",
 '' AS "G1NatID",
 '' AS "G1VotID",
 '' AS "G1DrivLic",
 '' AS "G1PassNum",
 '' AS "G1SSN",
 '' AS "G1Gender",
 '' AS "G1DOB",
 '' AS "G1Add1",
 '' AS "G1Add2",
 '' AS "G1Add3",
 '' AS "G1HomeTel",
 '' AS "G1WorkTel",
 '' AS "G1Mobile",
 '' AS "G2Surname",
 '' AS "G2FirstName",
 '' AS "G2MiddleNames",
 '' AS "G2NatID",
 '' AS "G2VotID",
 '' AS "G2DrivLic",
 '' AS "G2PassNum",
 '' AS "G2SSN",
 '' AS "G2Gender",
 '' AS "G2DOB",
 '' AS "G2Add1",
 '' AS "G2Add2",
 '' AS "G2Add3",
 '' AS "G2HomeTel",
 '' AS "G2WorkTel",
 '' AS "G2Mobile",
 '' AS "G3Surname",
 '' AS "G3FirstName",
 '' AS "G3MiddleNames",
 '' AS "G3NatID",
 '' AS "G3VotID",
 '' AS "G3DrivLic",
 '' AS "G3PassNum",
 '' AS "G3SSN",
 '' AS "G3Gender",
 '' AS "G3DOB",
 '' AS "G3Add1",
 '' AS "G3Add2",
 '' AS "G3Add3",
 '' AS "G3HomeTel",
 '' AS "G3WorkTel",
 '' AS "G3Mobile",
 '' AS "G4Surname",
 '' AS "G4FirstName",
 '' AS "G4MiddleNames",
 '' AS "G4NatID",
 '' AS "G4VotID",
 '' AS "G4DrivLic",
 '' AS "G4PassNum",
 '' AS "G4SSN",
 '' AS "G4Gender",
 '' AS "G4DOB",
 '' AS "G4Add1",
 '' AS "G4Add2",
 '' AS "G4Add3",
 '' AS "G4HomeTel",
 '' AS "G4WorkTel",
 '' AS "G4Mobile"
FROM ml.loan_info_tbl ml
JOIN mambu.client c ON ml.client_key = c.encodedkey
LEFT JOIN mambu.activity a_ ON a_.loanaccountkey = ml.loan_key
AND a_."type" IN ('LOAN_ACCOUNT_SET_TO_CLOSED_OBLIGATIONS_MET',
                  'LOAN_ACCOUNT_SET_TO_CLOSED_WRITTEN_OFF')
JOIN ml.repayment r ON ml.loan_key = r.loan_key
LEFT JOIN ml.next_repayment nr ON ml.loan_key = nr.loan_key
JOIN ml.LOAN_TRANSACTION_SUMMARY lt ON ml.loan_key = lt.loan_key
LEFT JOIN mambu.identificationdocument i_voter_id ON c.encodedkey = i_voter_id.clientkey
AND i_voter_id.documenttype = 'Voter ID'
LEFT JOIN mambu.identificationdocument i_nat_id ON c.encodedkey = i_nat_id.clientkey
AND i_nat_id.documenttype IN ('National ID',
                              'Ghana Card ID')
LEFT JOIN mambu.identificationdocument i_driver_id ON c.encodedkey = i_driver_id.clientkey
AND i_driver_id.documenttype = 'Driver''s License'
LEFT JOIN mambu.identificationdocument i_passport_id ON c.encodedkey = i_passport_id.clientkey
AND i_passport_id.documenttype = 'Passport'
LEFT JOIN mambu.identificationdocument i_nhia_id ON c.encodedkey = i_nhia_id.clientkey
AND i_nhia_id.documenttype = 'NHIA Membership'
LEFT JOIN mambu.address a ON c.ENCODEDKEY = a.PARENTKEY
LEFT JOIN mambu.customfieldvalue marital_status ON c.encodedkey = marital_status.PARENTKEY
AND marital_status.CUSTOMFIELDKEY =
  (SELECT encodedkey
   FROM mambu.customfield
   WHERE id = 'marital_status')
LEFT JOIN mambu.client_extra_values ccf ON c.encodedkey = ccf.client_key
LEFT JOIN mambu.customfieldvalue LOCATION ON c.ENCODEDKEY = location.PARENTKEY
AND location.CUSTOMFIELDKEY =
  (SELECT encodedkey
   FROM mambu.customfield
   WHERE id = 'location')
LEFT JOIN mambu.customfieldvalue home_owner ON c.ENCODEDKEY = home_owner.PARENTKEY
AND home_owner.CUSTOMFIELDKEY =
  (SELECT encodedkey
   FROM mambu.customfield
   WHERE id = 'home_owner')
LEFT JOIN mambu.customfieldvalue employment ON c.ENCODEDKEY = employment.PARENTKEY
AND employment.CUSTOMFIELDKEY =
  (SELECT encodedkey
   FROM mambu.customfield
   WHERE id = 'employment')
LEFT JOIN mambu.customfieldvalue employer_name ON c.ENCODEDKEY = employer_name.PARENTKEY
AND employer_name.CUSTOMFIELDKEY =
  (SELECT encodedkey
   FROM mambu.customfield
   WHERE id = 'employer_name')
LEFT JOIN mambu.customfieldvalue occupation ON c.ENCODEDKEY = occupation.PARENTKEY
AND occupation.CUSTOMFIELDKEY =
  (SELECT encodedkey
   FROM mambu.customfield
   WHERE id = 'occupation')
LEFT JOIN mambu.customfieldvalue POSITION ON c.ENCODEDKEY = position.PARENTKEY
AND position.CUSTOMFIELDKEY =
  (SELECT encodedkey
   FROM mambu.customfield
   WHERE id = 'position')
LEFT JOIN mambu.customfieldvalue income ON c.ENCODEDKEY = income.PARENTKEY
AND income.CUSTOMFIELDKEY =
  (SELECT encodedkey
   FROM mambu.customfield
   WHERE id = 'total_monthly_income') --- DEAL WITH WRITTEN OFFS -----
LEFT JOIN
  (SELECT recentmonth.loanid,
          recentmonth.entrydate lastpaiddate,
          recentmonth.amount lastpaidamount,
          (todate.cr_amount - todate.dr_amount) totalpaid,
          least((todate.cr_amount - todate.dr_amount) + writeoff.amount,0) revised_writeoff_amount
   FROM
     (SELECT entrydate,
             amount,
             right(trim(replace(notes, 'n', '')),7) loanid,
             row_number() OVER (PARTITION BY loanid
                                ORDER BY entrydate DESC) rid
      FROM mambu.gljournalentry gle
      LEFT JOIN mambu.glaccount gla ON gle.glaccount_encodedkey_oid = gla.encodedkey
      WHERE gla.name = 'Recovery from Write-Off'
        AND gle."type" = 'CREDIT'
        AND gle.creationdate::date BETWEEN '2020-06-30' AND CURRENT_DATE
      ORDER BY 1 DESC) recentmonth
   LEFT JOIN
     (SELECT right(trim(replace(notes, 'n', '')),7) loanid,
             nvl(sum(CASE
                         WHEN gle."type" = 'CREDIT' THEN amount
                     END),0.00) cr_amount,
             nvl(sum(CASE
                         WHEN gle."type" = 'DEBIT' THEN amount
                     END),0.00) dr_amount
      FROM mambu.gljournalentry gle
      LEFT JOIN mambu.glaccount gla ON gle.glaccount_encodedkey_oid = gla.encodedkey
      WHERE gla.name = 'Recovery from Write-Off'
        AND gle."type" IN ('CREDIT',
                           'DEBIT')
      GROUP BY 1) todate ON todate.loanid = recentmonth.loanid
   LEFT JOIN
     (SELECT l.id,
             lt.amount
      FROM mambu.loanaccount l,
           mambu.loantransaction lt
      WHERE lt.parentaccountkey = l.encodedkey
        AND lt."type" = 'WRITE_OFF'
        AND l.accountstate = 'CLOSED_WRITTEN_OFF'
        AND lt.reversaltransactionkey IS NULL) writeoff ON writeoff.id = recentmonth.loanid
   WHERE rid = 1
     AND least((todate.cr_amount - todate.dr_amount) + writeoff.amount,0) IS NOT NULL
   GROUP BY 1,
            2,
            3,
            4,
            5
   HAVING totalpaid > 0) AS recovered ON ml.loan_id = recovered.loanid
WHERE a_."type" IN ('LOAN_ACCOUNT_SET_TO_CLOSED_OBLIGATIONS_MET',
                    'LOAN_ACCOUNT_SET_TO_CLOSED_WRITTEN_OFF')
  AND CASE
          WHEN a_."type" = 'LOAN_ACCOUNT_SET_TO_CLOSED_OBLIGATIONS_MET' THEN a_."timestamp"::date BETWEEN '{{report_date_start}}'::date AND '{{report_date_end}}'::date
          ELSE recovered.lastpaiddate::date BETWEEN '{{report_date_start}}'::date AND '{{report_date_end}}'::date
      END