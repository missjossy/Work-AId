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

def analyze_client_duplicates(climate_df):
    """Analyze clients who appear multiple times across all insurance files"""
    print("\n" + "="*60)
    print("DUPLICATE CLIENT ANALYSIS - INSURANCE RESTART BUG DETECTION")
    print("="*60)
    
    if climate_df.empty:
        print("No insurance data available for analysis")
        return pd.DataFrame(), pd.DataFrame()
    
    # Group by CLIENT_ID and count occurrences
    client_counts = climate_df.groupby('CLIENT_ID').agg({
        'FULL_NAME': 'first',
        'GENDER': 'first',
        'ACCT_TYPE': 'first',
        'DISASTER_INSURANCE_CLIENT_TYPE': 'first',
        'LOANAMOUNT': 'first',
        'SOURCE_SHEET': 'count',
        'RECORD_MONTH': lambda x: list(set(x))  # Get unique months
    }).reset_index()
    
    # Rename columns for clarity
    client_counts.columns = ['CLIENT_ID', 'FULL_NAME', 'GENDER', 'ACCT_TYPE', 
                           'DISASTER_INSURANCE_CLIENT_TYPE', 'LOANAMOUNT', 
                           'APPEARANCE_COUNT', 'MONTHS_APPEARED']
    
    # Sort by appearance count (highest first)
    client_counts = client_counts.sort_values('APPEARANCE_COUNT', ascending=False)
    
    # Separate duplicate and single-appearance clients
    duplicate_clients = client_counts[client_counts['APPEARANCE_COUNT'] > 1]
    single_appearance_clients = client_counts[client_counts['APPEARANCE_COUNT'] == 1]
    
    print(f"\nTotal unique clients in insurance files: {len(client_counts)}")
    print(f"Clients appearing only once: {len(single_appearance_clients)}")
    print(f"Clients appearing multiple times (potential bug): {len(duplicate_clients)}")
    
    return duplicate_clients, single_appearance_clients

def detailed_duplicate_analysis(climate_df, duplicate_clients):
    """Perform detailed analysis of duplicate clients"""
    print("\n" + "="*60)
    print("DETAILED DUPLICATE CLIENT ANALYSIS")
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
    duplicate_records.to_csv('duplicate_insurance_records.csv', index=False)
    print(f"\nDetailed duplicate records saved to: duplicate_insurance_records.csv")
    
    # Save summary of duplicate clients
    duplicate_summary = duplicate_clients[['CLIENT_ID', 'FULL_NAME', 'APPEARANCE_COUNT', 'MONTHS_APPEARED', 'SHEETS_APPEARED']]
    duplicate_summary.to_csv('duplicate_clients_summary.csv', index=False)
    print(f"Duplicate clients summary saved to: duplicate_clients_summary.csv")
    
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

def generate_duplicate_summary_report(duplicate_clients, single_appearance_clients):
    """Generate a summary report for duplicate analysis"""
    print("\n" + "="*60)
    print("DUPLICATE ANALYSIS SUMMARY REPORT")
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
        
        # Show severity levels
        high_duplicates = duplicate_clients[duplicate_clients['APPEARANCE_COUNT'] >= 5]
        medium_duplicates = duplicate_clients[(duplicate_clients['APPEARANCE_COUNT'] >= 3) & (duplicate_clients['APPEARANCE_COUNT'] < 5)]
        low_duplicates = duplicate_clients[duplicate_clients['APPEARANCE_COUNT'] == 2]
        
        print(f"\n   Severity breakdown:")
        print(f"     High (5+ appearances): {len(high_duplicates)} clients")
        print(f"     Medium (3-4 appearances): {len(medium_duplicates)} clients")
        print(f"     Low (2 appearances): {len(low_duplicates)} clients")
        
        print(f"\n📊 Files generated:")
        print(f"   - duplicate_insurance_records.csv (detailed records)")
        print(f"   - clients_missing_from_climate_disaster.csv (if any)")
        print(f"   - clients_missing_from_results_table.csv (if any)")
    else:
        print("\n✅ No duplicate clients found - insurance data appears clean!")

def analyze_client_overlap(results_df, climate_df):
    """Analyze the overlap between the two datasets"""
    print("\n" + "="*60)
    print("CLIENT RECORD COMPARISON ANALYSIS")
    print("="*60)
    
    if climate_df.empty:
        print("No insurance data available for comparison")
        return set(), set(), set()
    
    # Get unique client IDs from each file
    results_clients = set(results_df['CLIENT_ID'])
    climate_clients = set(climate_df['CLIENT_ID'])
    
    # Find overlapping and non-overlapping clients
    common_clients = results_clients.intersection(climate_clients)
    only_in_results = results_clients - climate_clients
    only_in_climate = climate_clients - results_clients
    
    print(f"\nTotal unique clients in Results table: {len(results_clients)}")
    print(f"Total unique clients in Climate Disaster files: {len(climate_clients)}")
    print(f"Clients found in BOTH datasets: {len(common_clients)}")
    print(f"Clients ONLY in Results table: {len(only_in_results)}")
    print(f"Clients ONLY in Climate Disaster files: {len(only_in_climate)}")
    
    return common_clients, only_in_results, only_in_climate

def detailed_analysis(results_df, climate_df, common_clients, only_in_results, only_in_climate):
    """Perform detailed analysis of the differences"""
    print("\n" + "="*60)
    print("DETAILED ANALYSIS")
    print("="*60)
    
    # 1. Clients missing from Climate Disaster file (exist in Results but not Climate)
    print(f"\n1. CLIENTS MISSING FROM CLIMATE DISASTER FILE ({len(only_in_results)} records):")
    print("   (These clients exist in Results table but NOT in Climate Disaster file)")
    
    if only_in_results:
        missing_clients_df = results_df[results_df['CLIENT_ID'].isin(only_in_results)][
            ['CLIENT_ID', 'FULL_NAME', 'GENDER', 'ACCT_TYPE', 'DISASTER_INSURANCE_CLIENT_TYPE']
        ].sort_values('CLIENT_ID')
        
        print(f"\n   First 10 missing clients:")
        print(missing_clients_df.head(10).to_string(index=False))
        
        if len(only_in_results) > 10:
            print(f"\n   ... and {len(only_in_results) - 10} more clients")
        
        # Save to CSV
        missing_clients_df.to_csv('clients_missing_from_climate_disaster.csv', index=False)
        print(f"\n   Full list saved to: clients_missing_from_climate_disaster.csv")
    
    # 2. Clients missing from Results table (exist in Climate but not Results)
    print(f"\n2. CLIENTS MISSING FROM RESULTS TABLE ({len(only_in_climate)} records):")
    print("   (These clients exist in Climate Disaster file but NOT in Results table)")
    
    if only_in_climate:
        extra_clients_df = climate_df[climate_df['CLIENT_ID'].isin(only_in_climate)][
            ['CLIENT_ID', 'FULL_NAME', 'GENDER', 'ACCT_TYPE', 'DISASTER_INSURANCE_CLIENT_TYPE']
        ].sort_values('CLIENT_ID')
        
        print(f"\n   First 10 extra clients:")
        print(extra_clients_df.head(10).to_string(index=False))
        
        if len(only_in_climate) > 10:
            print(f"\n   ... and {len(only_in_climate) - 10} more clients")
        
        # Save to CSV
        extra_clients_df.to_csv('clients_missing_from_results_table.csv', index=False)
        print(f"\n   Full list saved to: clients_missing_from_results_table.csv")
    
    # 3. Summary statistics for common clients
    print(f"\n3. COMMON CLIENTS ANALYSIS ({len(common_clients)} records):")
    print("   (Clients found in BOTH files)")
    
    if common_clients:
        common_results = results_df[results_df['CLIENT_ID'].isin(common_clients)]
        common_climate = climate_df[climate_df['CLIENT_ID'].isin(common_clients)]
        
        print(f"   - Average loan amount in Results: ${common_results['LOANAMOUNT'].mean():.2f}")
        print(f"   - Average loan amount in Climate: ${common_climate['LOANAMOUNT'].mean():.2f}")
        
        # Check for any data discrepancies in common records
        print(f"\n   - Data consistency check:")
        
        # Compare key fields for common clients
        merged_common = pd.merge(
            common_results[['CLIENT_ID', 'FULL_NAME', 'LOANAMOUNT', 'ACCT_TYPE']], 
            common_climate[['CLIENT_ID', 'FULL_NAME', 'LOANAMOUNT', 'ACCT_TYPE']], 
            on='CLIENT_ID', 
            suffixes=('_results', '_climate')
        )
        
        # Check for name mismatches
        name_mismatches = merged_common[
            merged_common['FULL_NAME_results'] != merged_common['FULL_NAME_climate']
        ]
        if len(name_mismatches) > 0:
            print(f"     * {len(name_mismatches)} clients have different names between files")
        else:
            print(f"     * All common clients have matching names")
        
        # Check for loan amount differences
        loan_diff = merged_common[
            abs(merged_common['LOANAMOUNT_results'] - merged_common['LOANAMOUNT_climate']) > 0.01
        ]
        if len(loan_diff) > 0:
            print(f"     * {len(loan_diff)} clients have different loan amounts between files")
        else:
            print(f"     * All common clients have matching loan amounts")

def generate_summary_report(common_clients, only_in_results, only_in_climate):
    """Generate a summary report"""
    print("\n" + "="*60)
    print("SUMMARY REPORT")
    print("="*60)
    
    total_unique = len(common_clients) + len(only_in_results) + len(only_in_climate)
    
    print(f"\nTotal unique clients across both files: {total_unique}")
    print(f"Data consistency: {len(common_clients)/total_unique*100:.1f}% of clients appear in both files")
    
    if only_in_results:
        print(f"\n⚠️  {len(only_in_results)} clients need to be added to Climate Disaster file")
        print("   These clients exist in the Results table but are missing from Climate Disaster records")
    
    if only_in_climate:
        print(f"\n⚠️  {len(only_in_climate)} clients need to be added to Results table")
        print("   These clients exist in Climate Disaster records but are missing from Results table")
    
    if not only_in_results and not only_in_climate:
        print("\n✅ Perfect match! All clients appear in both files.")
    
    print(f"\n📊 Files generated:")
    if only_in_results:
        print("   - clients_missing_from_climate_disaster.csv")
    if only_in_climate:
        print("   - clients_missing_from_results_table.csv")

def main():
    """Main function to run the analysis"""
    try:
        # Load and clean data
        results_df, climate_df = load_and_clean_data()
        
        # Analyze duplicate clients (main focus for insurance restart bug)
        duplicate_clients, single_appearance_clients = analyze_client_duplicates(climate_df)
        
        # Perform detailed duplicate analysis
        detailed_duplicate_analysis(climate_df, duplicate_clients)
        
        # Generate duplicate summary report
        generate_duplicate_summary_report(duplicate_clients, single_appearance_clients)
        
        # Analyze overlap with results table
        common_clients, only_in_results, only_in_climate = analyze_client_overlap(results_df, climate_df)
        
        # Perform detailed analysis
        detailed_analysis(results_df, climate_df, common_clients, only_in_results, only_in_climate)
        
        # Generate summary report
        generate_summary_report(common_clients, only_in_results, only_in_climate)
        
        print(f"\n" + "="*60)
        print("ANALYSIS COMPLETE!")
        print("="*60)
        
    except FileNotFoundError as e:
        print(f"Error: Could not find one of the CSV files. Please ensure both files are in the current directory.")
        print(f"Missing file: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main() 