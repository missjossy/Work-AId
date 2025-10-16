SELECT 
    c.id AS "ClientID",
    c.firstName AS "FirstName",
    c.lastName AS "LastName",
    c.birthdate AS "DateOfBirth",
    TO_CHAR(c.birthdate, 'YYYYMMDD') AS "DOB_Formatted",
    DATEDIFF('year', c.birthdate, CURRENT_DATE()) AS "CalculatedAge",
    CASE 
        WHEN c.birthdate IS NULL THEN 'Missing DOB'
        WHEN c.birthdate > CURRENT_DATE() THEN 'Future DOB'
        WHEN DATEDIFF('year', c.birthdate, CURRENT_DATE()) < 0 THEN 'Negative Age'
        WHEN DATEDIFF('year', c.birthdate, CURRENT_DATE()) < 18 THEN 'Under 18'
        WHEN DATEDIFF('year', c.birthdate, CURRENT_DATE()) > 120 THEN 'Over 120'
        WHEN DATEDIFF('year', c.birthdate, CURRENT_DATE()) BETWEEN 18 AND 120 THEN 'Normal Age'
        ELSE 'Other Issue'
    END AS "AgeValidation",
    CASE 
        WHEN c.birthdate IS NULL THEN 'ERROR'
        WHEN c.birthdate > CURRENT_DATE() THEN 'ERROR'
        WHEN DATEDIFF('year', c.birthdate, CURRENT_DATE()) < 0 THEN 'ERROR'
        WHEN DATEDIFF('year', c.birthdate, CURRENT_DATE()) < 18 THEN 'WARNING'
        WHEN DATEDIFF('year', c.birthdate, CURRENT_DATE()) > 120 THEN 'ERROR'
        WHEN DATEDIFF('year', c.birthdate, CURRENT_DATE()) BETWEEN 18 AND 120 THEN 'OK'
        ELSE 'ERROR'
    END AS "Status"
FROM mambu.client c
WHERE 
    c.birthdate IS NULL OR
    c.birthdate > CURRENT_DATE() OR
    DATEDIFF('year', c.birthdate, CURRENT_DATE()) < 0 OR
    DATEDIFF('year', c.birthdate, CURRENT_DATE()) < 18 OR
    DATEDIFF('year', c.birthdate, CURRENT_DATE()) > 120
ORDER BY 
    CASE 
        WHEN c.birthdate IS NULL THEN 1
        WHEN c.birthdate > CURRENT_DATE() THEN 2
        WHEN DATEDIFF('year', c.birthdate, CURRENT_DATE()) < 0 THEN 3
        WHEN DATEDIFF('year', c.birthdate, CURRENT_DATE()) > 120 THEN 4
        WHEN DATEDIFF('year', c.birthdate, CURRENT_DATE()) < 18 THEN 5
        ELSE 6
    END,
    c.lastName,
    c.firstName; 