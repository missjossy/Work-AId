-- Comprehensive table schema export for ml.loan_info_tbl and mambu.client
-- Run this query and export results to CSV

WITH loan_info_schema AS (
    SELECT 
        'ml.loan_info_tbl' AS "TableName",
        column_name AS "ColumnName",
        data_type AS "DataType",
        character_maximum_length AS "MaxLength",
        is_nullable AS "IsNullable",
        column_default AS "DefaultValue",
        ordinal_position AS "ColumnOrder",
        COALESCE(c.comment, '') AS "ColumnDescription",
        COALESCE(t.comment, '') AS "TableDescription"
    FROM information_schema.columns c
    LEFT JOIN information_schema.tables t 
        ON c.schema = t.schema 
        AND c.name = t.name
    WHERE c.schema = 'ml' 
        AND c.table_name = 'loan_info_tbl'
),

client_schema AS (
    SELECT 
        'mambu.client' AS "TableName",
        column_name AS "ColumnName",
        data_type AS "DataType",
        character_maximum_length AS "MaxLength",
        is_nullable AS "IsNullable",
        column_default AS "DefaultValue",
        ordinal_position AS "ColumnOrder",
        COALESCE(c.comment, '') AS "ColumnDescription",
        COALESCE(t.comment, '') AS "TableDescription"
    FROM information_schema.columns c
    LEFT JOIN information_schema.tables t 
        ON c.table_schema = t.table_schema 
        AND c.table_name = t.table_name
    WHERE c.table_schema = 'mambu' 
        AND c.table_name = 'client'
)
select * from loan_info_schema;
-- select * from client_schema;
-- SELECT 
--     TableName,
--     ColumnName,
--     DataType,
--     MaxLength,
--     IsNullable,
--     DefaultValue,
--     ColumnOrder,
--     TableDescription,
--     ColumnDescription,
--     CURRENT_TIMESTAMP() AS "ExportDate"
-- FROM loan_info_schema

-- UNION ALL

-- SELECT 
--     TableName,
--     ColumnName,
--     DataType,
--     MaxLength,
--     IsNullable,
--     DefaultValue,
--     ColumnOrder,
--     TableDescription,
--     ColumnDescription,
--     CURRENT_TIMESTAMP() AS "ExportDate"
-- FROM client_schema

-- ORDER BY 
--     TableName,
--     ColumnOrder; 