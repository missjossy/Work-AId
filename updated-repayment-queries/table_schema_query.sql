-- Query to get table schema information for ml.loan_info_tbl
SELECT 
    'ml.loan_info_tbl' AS "TableName",
    column_name AS "ColumnName",
    data_type AS "DataType",
    character_maximum_length AS "MaxLength",
    is_nullable AS "IsNullable",
    column_default AS "DefaultValue",
    ordinal_position AS "ColumnOrder",
    'ml.loan_info_tbl' AS "Schema"
FROM information_schema.columns 
WHERE table_schema = 'ml' 
    AND table_name = 'loan_info_tbl'
ORDER BY ordinal_position

UNION ALL

-- Query to get table schema information for mambu.client
SELECT 
    'mambu.client' AS "TableName",
    column_name AS "ColumnName",
    data_type AS "DataType",
    character_maximum_length AS "MaxLength",
    is_nullable AS "IsNullable",
    column_default AS "DefaultValue",
    ordinal_position AS "ColumnOrder",
    'mambu' AS "Schema"
FROM information_schema.columns 
WHERE table_schema = 'mambu' 
    AND table_name = 'client'
ORDER BY ordinal_position; 