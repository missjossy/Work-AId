import pandas as pd
import numpy as np
import glob
import os
from datetime import datetime

def load_and_clean_data():
    """Load Excel file with monthly sheets and clean the data"""
    print("Loading Excel file with monthly insurance data...")
    
    # Find Excel files in the current directory
    excel_files = glob.glob('*.xlsx')
    insurance_files = [f for f in excel_files if 'Climate_Disaster_Insurance' in f]
    
    if not insurance_files:
        print("No Climate Disaster Insurance Excel files found!")
        return pd.DataFrame(), pd.DataFrame()
    
    # Load the results table
    results_df = pd.read_csv('result-Table 1.csv')
    print(f"Results table: {len(results_df)} records")
    
    # Load the main insurance Excel file
    insurance_file = insurance_files[0]
    print(f"Loading insurance data from: {insurance_file}")
    
    # Get all sheet names
    xl_file = pd.ExcelFile(insurance_file)
    sheet_names = xl_file.sheet_names
    print(f"Found {len(sheet_names)} monthly sheets: {sheet_names}")
    
    # Load all monthly sheets
    all_insurance_data = []
    for sheet_name in sheet_names:
        try:
            # Skip non-monthly sheets (if any)
            if not any(month in sheet_name for month in ["'25", "'24", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]):
                continue
                
            df = pd.read_excel(insurance_file, sheet_name=sheet_name)
            
            # Add source information
            df['SOURCE_SHEET'] = sheet_name
            df['RECORD_MONTH'] = sheet_name  # Use sheet name as month
            
            # Clean column names (remove extra whitespace)
            df.columns = df.columns.str.strip()
            
            all_insurance_data.append(df)
            print(f"  {sheet_name}: {len(df)} records")
            
        except Exception as e:
            print(f"  Error loading sheet {sheet_name}: {e}")
    
    # Combine all monthly data
    if all_insurance_data:
        climate_df = pd.concat(all_insurance_data, ignore_index=True)
        print(f"Combined insurance data: {len(climate_df)} total records")
        
        # Show unique months loaded
        unique_months = climate_df['RECORD_MONTH'].unique()
        print(f"Months loaded: {sorted(unique_months)}")
    else:
        climate_df = pd.DataFrame()
        print("No monthly sheets loaded successfully")
    
    # Clean CLIENT_ID column - remove any whitespace and convert to string
    results_df['CLIENT_ID'] = results_df['CLIENT_ID'].astype(str).str.strip()
    if not climate_df.empty:
        climate_df['CLIENT_ID'] = climate_df['CLIENT_ID'].astype(str).str.strip()
    
    return results_df, climate_df

def analyze_client_duplicates_enhanced(climate_df):
    """Enhanced analysis of clients who appear multiple times across all insurance files"""
    print("\n" + "="*60)
    print("ENHANCED DUPLICATE CLIENT ANALYSIS - INSURANCE RESTART BUG DETECTION")
    print("="*60)
    
    if climate_df.empty:
        print("No insurance data available for analysis")
        return pd.DataFrame(), pd.DataFrame()
    
    # First, let's verify the counting logic by doing a direct count
    print("Verifying client appearance counts...")
    direct_counts = climate_df['CLIENT_ID'].value_counts()
    print(f"Direct count verification - Total records: {len(climate_df)}")
    print(f"Direct count verification - Unique clients: {len(direct_counts)}")
    
    # Show the actual distribution
    print(f"\nActual appearance distribution:")
    count_distribution = direct_counts.value_counts().sort_index()
    for count, freq in count_distribution.items():
        print(f"  {count} appearance(s): {freq} clients")
    
    # Get clients with more than 1 appearance
    duplicate_client_ids = direct_counts[direct_counts > 1].index.tolist()
    single_client_ids = direct_counts[direct_counts == 1].index.tolist()
    
    print(f"\nClients with multiple appearances: {len(duplicate_client_ids)}")
    print(f"Clients with single appearance: {len(single_client_ids)}")
    
    # Create detailed analysis for duplicate clients
    duplicate_clients_data = []
    for client_id in duplicate_client_ids:
        client_records = climate_df[climate_df['CLIENT_ID'] == client_id]
        
        # Get all the months and sheets this client appears in
        months = sorted(client_records['RECORD_MONTH'].unique())
        sheets = sorted(client_records['SOURCE_SHEET'].unique())
        
        duplicate_clients_data.append({
            'CLIENT_ID': client_id,
            'FULL_NAME': client_records.iloc[0]['FULL_NAME'],
            'GENDER': client_records.iloc[0]['GENDER'],
            'ACCT_TYPE': client_records.iloc[0]['ACCT_TYPE'],
            'DISASTER_INSURANCE_CLIENT_TYPE': client_records.iloc[0]['DISASTER_INSURANCE_CLIENT_TYPE'],
            'LOANAMOUNT': client_records.iloc[0]['LOANAMOUNT'],
            'APPEARANCE_COUNT': len(client_records),
            'MONTHS_APPEARED': months,
            'SHEETS_APPEARED': sheets,
            'TOTAL_RECORDS': len(client_records)
        })
    
    duplicate_clients = pd.DataFrame(duplicate_clients_data)
    duplicate_clients = duplicate_clients.sort_values('APPEARANCE_COUNT', ascending=False)
    
    # Create single appearance clients dataframe
    single_appearance_clients = climate_df[climate_df['CLIENT_ID'].isin(single_client_ids)][
        ['CLIENT_ID', 'FULL_NAME', 'GENDER', 'ACCT_TYPE', 'DISASTER_INSURANCE_CLIENT_TYPE', 'LOANAMOUNT']
    ].drop_duplicates(subset=['CLIENT_ID'])
    
    return duplicate_clients, single_appearance_clients

def detailed_duplicate_analysis_enhanced(climate_df, duplicate_clients):
    """Enhanced detailed analysis of duplicate clients"""
    print("\n" + "="*60)
    print("ENHANCED DETAILED DUPLICATE CLIENT ANALYSIS")
    print("="*60)
    
    if duplicate_clients.empty:
        print("No duplicate clients found.")
        return
    
    # Show ALL duplicate clients with their details
    print(f"\nALL DUPLICATE CLIENTS ({len(duplicate_clients)} total):")
    print("="*80)
    
    for idx, client in duplicate_clients.iterrows():
        print(f"\nClient {idx+1}: {client['CLIENT_ID']} - {client['FULL_NAME']}")
        print(f"  Appearances: {client['APPEARANCE_COUNT']}")
        print(f"  Months: {client['MONTHS_APPEARED']}")
        print(f"  Sheets: {client['SHEETS_APPEARED']}")
        print(f"  Account Type: {client['ACCT_TYPE']}")
        print(f"  Insurance Type: {client['DISASTER_INSURANCE_CLIENT_TYPE']}")
        print(f"  Loan Amount: ${client['LOANAMOUNT']:,.2f}")
        print("-" * 60)
    
    # Get detailed records for duplicate clients
    duplicate_client_ids = duplicate_clients['CLIENT_ID'].tolist()
    duplicate_records = climate_df[climate_df['CLIENT_ID'].isin(duplicate_client_ids)].copy()
    
    # Sort by CLIENT_ID and SOURCE_SHEET for better analysis
    duplicate_records = duplicate_records.sort_values(['CLIENT_ID', 'SOURCE_SHEET'])
    
    # Save detailed duplicate records
    duplicate_records.to_csv('duplicate_insurance_records_enhanced.csv', index=False)
    print(f"\nDetailed duplicate records saved to: duplicate_insurance_records_enhanced.csv")
    
    # Save summary of duplicate clients
    duplicate_summary = duplicate_clients[['CLIENT_ID', 'FULL_NAME', 'APPEARANCE_COUNT', 'MONTHS_APPEARED', 'SHEETS_APPEARED']]
    duplicate_summary.to_csv('duplicate_clients_summary_enhanced.csv', index=False)
    print(f"Duplicate clients summary saved to: duplicate_clients_summary_enhanced.csv")
    
    # Analyze patterns
    print(f"\nPattern Analysis:")
    
    # Count by appearance frequency
    appearance_counts = duplicate_clients['APPEARANCE_COUNT'].value_counts().sort_index()
    print(f"\nAppearance frequency distribution:")
    for count, freq in appearance_counts.items():
        print(f"  {count} appearances: {freq} clients")
    
    # Check for clients with different months
    clients_with_multiple_months = duplicate_clients[
        duplicate_clients['MONTHS_APPEARED'].apply(lambda x: len(x) > 1)
    ]
    print(f"\nClients appearing in multiple months: {len(clients_with_multiple_months)}")
    
    if len(clients_with_multiple_months) > 0:
        print(f"\nClients appearing in multiple months:")
        for _, client in clients_with_multiple_months.iterrows():
            print(f"  {client['CLIENT_ID']} - {client['FULL_NAME']}: {client['MONTHS_APPEARED']}")
    
    # Show monthly breakdown for duplicate clients
    print(f"\nMonthly breakdown of duplicate clients:")
    monthly_duplicates = climate_df[climate_df['CLIENT_ID'].isin(duplicate_client_ids)]['RECORD_MONTH'].value_counts().sort_index()
    for month, count in monthly_duplicates.items():
        print(f"  {month}: {count} records")
    
    # Show sheet breakdown for duplicate clients
    print(f"\nSheet breakdown of duplicate clients:")
    sheet_duplicates = climate_df[climate_df['CLIENT_ID'].isin(duplicate_client_ids)]['SOURCE_SHEET'].value_counts().sort_index()
    for sheet, count in sheet_duplicates.items():
        print(f"  {sheet}: {count} records")
    
    # Verify the counts by showing a few examples
    print(f"\nVerification - Sample duplicate clients with all their records:")
    sample_clients = duplicate_clients.head(5)
    for _, client in sample_clients.iterrows():
        client_id = client['CLIENT_ID']
        client_records = climate_df[climate_df['CLIENT_ID'] == client_id]
        print(f"\n  {client_id} - {client['FULL_NAME']} (Expected: {client['APPEARANCE_COUNT']}, Actual: {len(client_records)})")
        for _, record in client_records.iterrows():
            print(f"    Sheet: {record['SOURCE_SHEET']}, Month: {record['RECORD_MONTH']}, Loan: ${record['LOANAMOUNT']:,.2f}")
    
    # Show verification summary
    print(f"\n✅ Verification Summary:")
    print(f"  - Total duplicate records: {duplicate_clients['APPEARANCE_COUNT'].sum()}")
    print(f"  - Average appearances per duplicate client: {duplicate_clients['APPEARANCE_COUNT'].mean():.2f}")
    print(f"  - Maximum appearances: {duplicate_clients['APPEARANCE_COUNT'].max()}")
    print(f"  - Minimum appearances: {duplicate_clients['APPEARANCE_COUNT'].min()}")
    
    # Show clients with highest counts
    max_appearances = duplicate_clients['APPEARANCE_COUNT'].max()
    if max_appearances > 2:
        high_duplicates = duplicate_clients[duplicate_clients['APPEARANCE_COUNT'] >= 3]
        print(f"\n🚨 HIGH PRIORITY - Clients with 3+ appearances ({len(high_duplicates)} total):")
        for _, client in high_duplicates.iterrows():
            print(f"  {client['CLIENT_ID']} - {client['FULL_NAME']}: {client['APPEARANCE_COUNT']} times in {client['MONTHS_APPEARED']}")

def generate_enhanced_summary_report(duplicate_clients, single_appearance_clients):
    """Generate an enhanced summary report for duplicate analysis"""
    print("\n" + "="*60)
    print("ENHANCED DUPLICATE ANALYSIS SUMMARY REPORT")
    print("="*60)
    
    total_clients = len(duplicate_clients) + len(single_appearance_clients)
    
    if total_clients > 0:
        duplicate_percentage = len(duplicate_clients) / total_clients * 100
        print(f"\nTotal unique clients: {total_clients}")
        print(f"Duplicate clients: {len(duplicate_clients)} ({duplicate_percentage:.1f}%)")
        print(f"Single appearance clients: {len(single_appearance_clients)} ({100-duplicate_percentage:.1f}%)")
    
    if not duplicate_clients.empty:
        print(f"\n🚨 INSURANCE RESTART BUG DETECTED!")
        print(f"   {len(duplicate_clients)} clients have multiple insurance records")
        print(f"   This suggests the bug where insurance data was restarted for new loans")
        
        # Show severity levels with more detailed breakdown
        max_appearances = duplicate_clients['APPEARANCE_COUNT'].max()
        print(f"\n   Maximum appearances for any client: {max_appearances}")
        
        # Show detailed breakdown
        print(f"\n   Detailed breakdown by appearance count:")
        appearance_breakdown = duplicate_clients['APPEARANCE_COUNT'].value_counts().sort_index()
        for count, freq in appearance_breakdown.items():
            print(f"     {count} appearance(s): {freq} clients")
        
        # Show clients with highest counts
        if max_appearances > 2:
            high_duplicates = duplicate_clients[duplicate_clients['APPEARANCE_COUNT'] >= 3]
            print(f"\n   Clients with 3+ appearances ({len(high_duplicates)} total):")
            for _, client in high_duplicates.iterrows():
                print(f"     {client['CLIENT_ID']} - {client['FULL_NAME']}: {client['APPEARANCE_COUNT']} times in {client['MONTHS_APPEARED']}")
        
        print(f"\n📊 Files generated:")
        print(f"   - duplicate_insurance_records_enhanced.csv (all duplicate records)")
        print(f"   - duplicate_clients_summary_enhanced.csv (summary by client)")
        
        # Show verification summary
        print(f"\n✅ Verification:")
        print(f"   - Total duplicate records: {duplicate_clients['APPEARANCE_COUNT'].sum()}")
        print(f"   - Average appearances per duplicate client: {duplicate_clients['APPEARANCE_COUNT'].mean():.2f}")
        
    else:
        print("\n✅ No duplicate clients found - insurance data appears clean!")

def main():
    """Main function to run the enhanced analysis"""
    try:
        # Load and clean data
        results_df, climate_df = load_and_clean_data()
        
        # Analyze duplicate clients with enhanced method
        duplicate_clients, single_appearance_clients = analyze_client_duplicates_enhanced(climate_df)
        
        # Perform detailed duplicate analysis
        detailed_duplicate_analysis_enhanced(climate_df, duplicate_clients)
        
        # Generate enhanced summary report
        generate_enhanced_summary_report(duplicate_clients, single_appearance_clients)
        
        print(f"\n" + "="*60)
        print("ENHANCED ANALYSIS COMPLETE!")
        print("="*60)
        
    except FileNotFoundError as e:
        print(f"Error: Could not find one of the required files. Please ensure both files are in the current directory.")
        print(f"Missing file: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
