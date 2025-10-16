SELECT 
    fs.client_id AS "ClientID",
    c.firstName AS "FirstName",
    c.lastName AS "LastName",
    c.mobilePhone1 AS "MobileNumber",
    fs.fido_score AS "FirstFidoScore",
    fs.score_date AS "FirstScoreDate",
    TO_CHAR(fs.score_date, 'YYYY-MM-DD') AS "FormattedScoreDate",
    fs.created_at AS "ScoreCreatedAt",
    fs.updated_at AS "ScoreUpdatedAt",
    CASE 
        WHEN fs.fido_score >= 800 THEN 'Excellent'
        WHEN fs.fido_score >= 700 THEN 'Good'
        WHEN fs.fido_score >= 600 THEN 'Fair'
        WHEN fs.fido_score >= 500 THEN 'Poor'
        ELSE 'Very Poor'
    END AS "ScoreCategory",
    DATEDIFF('day', fs.score_date, CURRENT_DATE()) AS "DaysSinceFirstScore",
    ROW_NUMBER() OVER (PARTITION BY fs.client_id ORDER BY fs.score_date ASC) AS "ScoreRank"
FROM data.fido_score fs
JOIN mambu.client c ON fs.client_id = c.id
WHERE fs.score_date = (
    SELECT MIN(fs2.score_date) 
    FROM data.fido_score fs2 
    WHERE fs2.client_id = fs.client_id
)
ORDER BY 
    fs.score_date ASC,
    c.lastName ASC,
    c.firstName ASC; 