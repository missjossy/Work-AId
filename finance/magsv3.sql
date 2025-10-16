with client_industries AS (
    SELECT 
        client_id, 
        cust_location, 
        INDUSTRY, 
        POSITION, 
        GENDER, 
        USE_OF_FUNDS, 
        EMPLOYMENT, 
        LOAN_DATE, 
        INCOME_VALUE, 
        DEPENDENTS,
        DATEDIFF(year, BIRTHDAY, CURRENT_DATE) AS Age,
        CASE 
            WHEN regions = 'Brong-Ahafo Region' THEN 
                CASE 
                    WHEN RANDOM() <= 0.4652 THEN 'Bono Region'
                    WHEN RANDOM() <= 0.7561 THEN 'Bono East Region'
                    ELSE 'Ahafo Region'
                END
            ELSE regions 
        END as region 
    FROM ( select *, CASE 
            WHEN region IN ('Accra', 'Tema') THEN 'Greater Accra Region'
            WHEN region = 'Other Region' THEN cust_location|| ' '|| 'Region' else region end as regions
    from data.survey_data
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CLIENT_ID ORDER BY LOAN_DATE DESC) = 1)
),
national_id AS (
	SELECT USER.id, BANKING_PLATFORM_ID, USER.NATIONAL_ID, CREATED_TIMESTAMP 
	FROM GHANA_PROD.BANKING_SERVICE.USER 
	QUALIFY row_number() OVER(PARTITION BY BANKING_PLATFORM_ID ORDER BY JSON_EXTRACT_PATH_TEXT(status,'status') ASC, USER.CREATED_TIMESTAMP DESC) = 1
),

mlpscl as 
(
    select date_trunc('day',date('2025-07-31')) day,
            MONTH as eomonth,
            datediff(days, date(m.last_expected_repayment), least(MONTH,CURRENT_DATE)) as days_late, 
            par,
           l.parentaccountkey,
           l.principalbalance,
           case when l.principalbalance < m.loanamount then l.principalbalance else m.loanamount end new_balance,
           l.balance,
           m.loan_id,
           m.loan_key,
           m.LAST_EXPECTED_REPAYMENT,
           m.client_key,
           c.id AS client_mambu_id,
           c.FIRSTNAME,
           c.MIDDLENAME,
           c.LASTNAME,
           c.GENDER,
           c.BIRTHDATE,
           nid.NATIONAL_ID,
           m.ln, 
           m.disbursementdate,
           m.APPROVEDDATE,
           m.REPAYMENTINSTALLMENTS,
           re.first_due_date,
           re.loan_interest_expected,
           l.transactionid,
           lp.id AS product_id,
           m.loanamount,
           m.principalpaid,
           s.industry,
           s.position,
           s.use_of_funds,
           s.employment,
           s.Age,
           s.INCOME_VALUE,
           s.DEPENDENTS,
           nvl(JSON_EXTRACT_PATH_TEXT(CUSTOM_FIELDS,'region'),s.region) as region,
           la.interestrate,
           CASE WHEN "type" = 'REPAYMENT' THEN l.INTERESTAMOUNT ELSE 0 END AS INTEREST_PAID,
           sum(INTEREST_PAID) OVER(PARTITION BY l.PARENTACCOUNTKEY ORDER BY l.TRANSACTIONID) AS CUMULATIVE_INTEREST_PAID,
           case when l."type" in ('REPAYMENT', 'REPAYMENT_ADJUSTMENT') then l.amount else 0 end repaid,
           sum(repaid) over (partition by l.parentaccountkey order by l.transactionid asc rows unbounded preceding) repayments,
           row_number() over (partition by l.parentaccountkey order by l.transactionid desc) row1
    from GHANA_PROD.ML.LOAN_INFO_TBL m 
    JOIN GHANA_PROD.MAMBU.CLIENT c ON c.ENCODEDKEY = m.CLIENT_KEY
    join GHANA_PROD.MAMBU.loantransaction l on m.loan_key = l.parentaccountkey
    LEFT JOIN
                 (SELECT  parentaccountkey, MAX(last_day(date(creationdate))) MONTH
                  FROM mambu.loantransaction
                  WHERE date(creationdate)  <= '2025-07-31'
                  group by 1
                  ) tt ON l.parentaccountkey = tt.parentaccountkey
    left join client_industries s on s.client_id = c.id
    LEFT JOIN national_id nid ON nid.BANKING_PLATFORM_ID = c.id
    join GHANA_PROD.MAMBU.LOANPRODUCT lp on m.producttypekey = lp.encodedkey
    join GHANA_PROD.MAMBU.loanaccount la on la.encodedkey = m.loan_key
    LEFT JOIN (SELECT PARENTACCOUNTKEY, min(DUEDATE) AS first_due_date, sum(INTERESTDUE) AS loan_interest_expected 
    FROM GHANA_PROD.MAMBU.REPAYMENT GROUP BY PARENTACCOUNTKEY) re ON re.PARENTACCOUNTKEY = m.LOAN_KEY
    left join GHANA_PROD.mambu.client_extra_values ce ON c.encodedkey = ce.client_key 
    left join  gh_dev.analytics_models.par_analysis par on par.loan_id = m.loan_id --par --on par.loan_id = m.loan_id
    where date(l.creationdate) <= DAY
    ),
    output as (
select client_mambu_id AS "Customer ID",
       'Individual' AS "Customer Type",
       'N/A' AS "Company Name",
        FIRSTNAME AS "First Name",
        MIDDLENAME AS "Middle Name",
        LASTNAME AS "Surname",
        GENDER AS "Gender",
        Age,
        INCOME_VALUE AS "Income Range",
        DEPENDENTS AS "Number of Dependents",
        'No' AS "Physically Challenged",
        '' AS "TIN",
        NATIONAL_ID AS "Ghana Card ID Number",
        date(BIRTHDATE) AS "Date Of Birth",
        region AS "Region",
        'Resident and Non Resident' AS "Residency",
        'No' AS "Related Party",
        'Others' AS "Institution Type",
        employment AS "Emplyment Status",
        CASE 
        WHEN INDUSTRY IN ('Agriculture', 'Fishing') THEN 'Agriculture Forestry & Fishing'
        WHEN INDUSTRY IN ('Mining') THEN 'Mining & Quarrying'
        WHEN INDUSTRY IN ('Industry/Manufacturing', 'Artisan', 'Textiles and Garment Making') THEN 'Manufacturing'
        WHEN INDUSTRY IN ('Construction') THEN 'Construction'
        WHEN INDUSTRY IN ('Oil & Gas', 'Electricity and Utilities') THEN 'Electricity, Gas & Water'
        WHEN INDUSTRY IN ('Trade - Wholesale', 'Trade - Retail', 'Trade - Import/Export', 'Banking and Financial Services', 'Food Industry', 'Beverage Industry', 'Sea Food Industry') THEN 'Commerce & Finance'
        WHEN INDUSTRY IN ('Transportation', 'Media and Communications', 'Logistics') THEN 'Transport, Storage And Communication'
        WHEN INDUSTRY IN ('Domestic Services', 'Other Community Services', 'Advisory/Consulting') THEN 'Services'
        ELSE 'Miscellaneous' 
        END AS "Economic Sector",
        '' AS "Institutional Distribution",
        loan_id AS "Account Number",
        'GHS' AS "Currency",
        loanamount AS "Loan Amount Approved",
        '' AS "Revaluation Rate",
        '' AS "Revaluation Amount (GHS Equivalent)",
        date(APPROVEDDATE) AS "Date of Approval",
        loanamount AS "Loan Amount Disbursed",
        date(disbursementdate) AS "Loan Disbursement Date",
        date(LAST_EXPECTED_REPAYMENT) AS "Loan Maturity Date",
        'DYNAMIC_TERM_LOAN' AS "Loan Product",
        'Loan' AS "Loan Category",
        '' AS "Loan Type",
        '' AS "Loan Purpose",
        'Monthly' AS "Frequency of Payments",
        interestrate AS "Interest Rate",
        'Fixed' AS "Interest Charge Type",
        REPAYMENTINSTALLMENTS AS "Number of Payments Agreed",
        date(first_due_date) AS "Agreed Date of First Principal Repayment",
        new_balance AS "Loan Principal Balance (Without Interest)",
        date(LAST_EXPECTED_REPAYMENT) AS "Date of Last Actual Principal Repayment",
        principalpaid AS "Amount of Last Actual Principal Repayment",
        '' AS "Date of Last Restructuring",
        CASE
                 WHEN par IS NULL THEN days_late
                 ELSE par
             END AS "Days in Arrears",
        CASE 
        WHEN ("Days in Arrears" <= 2 OR "Days in Arrears" IS NULL) THEN 'current' 
        WHEN "Days in Arrears" BETWEEN 3 AND 32 THEN 'par 0'
        WHEN "Days in Arrears" BETWEEN 33 AND 62 THEN 'par 30' 
        WHEN "Days in Arrears" BETWEEN 63 AND 92 THEN 'par 60'
        WHEN "Days in Arrears" BETWEEN 93 AND 122 THEN 'par 90'
        WHEN "Days in Arrears" BETWEEN 123 AND 152 THEN 'par 120'
        ELSE 'par 150' 
    END AS "BOG Loan Classification",
        CASE 
        WHEN ("Days in Arrears" <= 2 OR "Days in Arrears" IS NULL) THEN 0.01
        WHEN "Days in Arrears" BETWEEN 3 AND 32 THEN 0.01
        WHEN "Days in Arrears" BETWEEN 33 AND 62 THEN 0.2
        WHEN "Days in Arrears" BETWEEN 63 AND 92 THEN 0.4
        WHEN "Days in Arrears" BETWEEN 93 AND 122 THEN 0.6
        WHEN "Days in Arrears" BETWEEN 123 AND 152 THEN 0.8
        ELSE 1
    END AS "BOG Provision",
		'N/A' AS "IFRS Provision",
		case when ("Days in Arrears"<=60 or "Days in Arrears" is null) then 'Stage 1' 
              when "Days in Arrears" between 61 and 120 then 'Stage 2'
              else 'Stage 3' 
        end AS "IFRS Classification",
		CUMULATIVE_INTEREST_PAID AS "Interest Received",
		loan_interest_expected AS "Interest Receivable",
		'' AS "Write-Offs",
		'' AS "Recoveries",
		'' AS "Type of Security",
		'' AS "Descriptopn of Security",
		'' AS "Date of Valuation",
		'' AS "Value of Security",
		'' AS "Value of Allowable Security",
		'' AS "Force Sale Value of Security",
		'' AS "Security Interest (Charge) Registration Number",
		'' AS "Collateral Provider",
		'' AS "Collateral Searched with the Collateral Registery",
		'' AS "Status of Collateral",
		'' AS "Guarantor",
		'' AS "Credit Reference Bureau Checked",
		'Yes' AS "Submitted to Credit Reference Bureau",
		'' AS "Branch Number/Code",
		'Osu/App' AS "Branch Name",
		'Amy Afele' AS "Loan Officer",
		'Sebastian Quansah' AS "Branch Manager"
from mlpscl
where row1 = 1 and new_balance > 0 )

select *  from output