import pandas as pd
import snowflake.connector
import os
from datetime import datetime

# Database connection parameters (you'll need to update these)
DB_CONFIG = {
    'user': os.getenv('SNOWFLAKE_USER', 'your_username'),
    'password': os.getenv('SNOWFLAKE_PASSWORD', 'your_password'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT', 'your_account'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'your_warehouse'),
    'database': os.getenv('SNOWFLAKE_DATABASE', 'your_database'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA', 'your_schema')
}

def connect_to_snowflake():
    """Establish connection to Snowflake database"""
    try:
        conn = snowflake.connector.connect(**DB_CONFIG)
        print("Successfully connected to Snowflake")
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        return None

def get_table_schema(conn, table_name):
    """Get schema information for a specific table"""
    query = f"""
    SELECT 
        column_name AS "ColumnName",
        data_type AS "DataType",
        character_maximum_length AS "MaxLength",
        is_nullable AS "IsNullable",
        column_default AS "DefaultValue",
        ordinal_position AS "ColumnOrder",
        CASE 
            WHEN table_schema = 'ml' THEN 'ml.loan_info_tbl'
            WHEN table_schema = 'mambu' THEN 'mambu.client'
        END AS "TableName"
    FROM information_schema.columns 
    WHERE table_name = '{table_name.split('.')[-1]}'
        AND table_schema = '{table_name.split('.')[0]}'
    ORDER BY ordinal_position
    """
    
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        print(f"Error executing query for {table_name}: {e}")
        return pd.DataFrame()

def generate_csv_report():
    """Generate CSV report with table schemas"""
    conn = connect_to_snowflake()
    if not conn:
        return
    
    try:
        # Get schema for both tables
        loan_info_schema = get_table_schema(conn, 'ml.loan_info_tbl')
        client_schema = get_table_schema(conn, 'mambu.client')
        
        # Create timestamp for filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"table_schema_report_{timestamp}.csv"
        
        # Combine both schemas
        combined_schema = pd.concat([loan_info_schema, client_schema], ignore_index=True)
        
        # Add additional metadata
        combined_schema['GeneratedDate'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        combined_schema['Description'] = ''  # Placeholder for manual descriptions
        
        # Reorder columns for better readability
        column_order = [
            'TableName', 'ColumnName', 'DataType', 'MaxLength', 
            'IsNullable', 'DefaultValue', 'ColumnOrder', 'Description', 'GeneratedDate'
        ]
        combined_schema = combined_schema[column_order]
        
        # Save to CSV
        combined_schema.to_csv(filename, index=False)
        print(f"Schema report generated successfully: {filename}")
        
        # Print summary
        print(f"\nSummary:")
        print(f"ml.loan_info_tbl columns: {len(loan_info_schema)}")
        print(f"mambu.client columns: {len(client_schema)}")
        print(f"Total columns: {len(combined_schema)}")
        
        # Display sample of each table
        print(f"\nSample columns from ml.loan_info_tbl:")
        print(loan_info_schema[['ColumnName', 'DataType', 'IsNullable']].head())
        
        print(f"\nSample columns from mambu.client:")
        print(client_schema[['ColumnName', 'DataType', 'IsNullable']].head())
        
    except Exception as e:
        print(f"Error generating report: {e}")
    finally:
        conn.close()
        print("Database connection closed")

if __name__ == "__main__":
    generate_csv_report() 