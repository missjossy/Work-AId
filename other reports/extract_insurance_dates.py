import pandas as pd
from datetime import datetime

def extract_and_rank_insurance_dates():
    """Extract specific fields and rank by insurance start date for each client"""
    
    try:
        # Load the duplicate insurance records
        print("Loading duplicate insurance records...")
        df = pd.read_csv('duplicate_insurance_records.csv')
        print(f"Loaded {len(df)} records")
        
        # Select only the required columns
        required_columns = ['CLIENT_ID', 'ACCT_TYPE', 'DISASTER_INSURANCE_START_DATE', 'RECORD_MONTH']
        df_selected = df[required_columns].copy()
        
        # Clean and convert the insurance start date
        print("Processing insurance start dates...")
        df_selected['DISASTER_INSURANCE_START_DATE'] = pd.to_datetime(df_selected['DISASTER_INSURANCE_START_DATE'])
        
        # Sort by CLIENT_ID and then by insurance start date (ascending)
        df_sorted = df_selected.sort_values(['CLIENT_ID', 'DISASTER_INSURANCE_START_DATE'])
        
        # Add a rank column for each client (1 = earliest, 2 = second earliest, etc.)
        df_sorted['RANK'] = df_sorted.groupby('CLIENT_ID').cumcount() + 1
        
        # Reorder columns for better readability
        final_columns = ['CLIENT_ID', 'RANK', 'ACCT_TYPE', 'DISASTER_INSURANCE_START_DATE', 'RECORD_MONTH']
        df_final = df_sorted[final_columns].copy()
        
        # Format the date for better readability
        df_final['DISASTER_INSURANCE_START_DATE'] = df_final['DISASTER_INSURANCE_START_DATE'].dt.strftime('%Y-%m-%d')
        
        # Save the ranked results
        output_file = 'insurance_dates_ranked.csv'
        df_final.to_csv(output_file, index=False)
        print(f"\nRanked insurance dates saved to: {output_file}")
        
        # Display summary statistics
        print(f"\nSummary:")
        print(f"  - Total records: {len(df_final)}")
        print(f"  - Unique clients: {df_final['CLIENT_ID'].nunique()}")
        print(f"  - Date range: {df_final['DISASTER_INSURANCE_START_DATE'].min()} to {df_final['DISASTER_INSURANCE_START_DATE'].max()}")
        
        # Show the ranking distribution
        rank_distribution = df_final['RANK'].value_counts().sort_index()
        print(f"\nRanking distribution:")
        for rank, count in rank_distribution.items():
            print(f"  Rank {rank}: {count} records")
        
        # Show sample of the ranked data
        print(f"\nSample of ranked insurance dates (first 20 records):")
        print("="*80)
        print(df_final.head(20).to_string(index=False))
        
        # Show clients with their complete ranking
        print(f"\nComplete ranking for first 10 clients:")
        print("="*80)
        sample_clients = df_final['CLIENT_ID'].unique()[:10]
        for client_id in sample_clients:
            client_records = df_final[df_final['CLIENT_ID'] == client_id]
            print(f"\nClient {client_id}:")
            for _, record in client_records.iterrows():
                print(f"  Rank {record['RANK']}: {record['DISASTER_INSURANCE_START_DATE']} - {record['RECORD_MONTH']} - {record['ACCT_TYPE']}")
        
        return df_final
        
    except FileNotFoundError:
        print("Error: duplicate_insurance_records.csv not found. Please run the main analysis first.")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def analyze_insurance_patterns(df):
    """Analyze patterns in the insurance dates"""
    if df is None:
        return
    
    print(f"\n" + "="*60)
    print("INSURANCE PATTERN ANALYSIS")
    print("="*60)
    
    # Convert back to datetime for analysis
    df['DISASTER_INSURANCE_START_DATE'] = pd.to_datetime(df['DISASTER_INSURANCE_START_DATE'])
    
    # Count rank 2's (second insurance records) by month
    print(f"\nRANK 2 ANALYSIS - Second Insurance Records by Month:")
    print("="*60)
    
    rank2_records = df[df['RANK'] == 2]
    if not rank2_records.empty:
        rank2_by_month = rank2_records['RECORD_MONTH'].value_counts().sort_index()
        
        print(f"Total clients with second insurance records: {len(rank2_records)}")
        print(f"\nSecond insurance records by month:")
        for month, count in rank2_by_month.items():
            print(f"  {month}: {count} clients")
        
        # Save rank 2 analysis
        rank2_file = 'rank2_insurance_by_month.csv'
        rank2_records.to_csv(rank2_file, index=False)
        print(f"\nRank 2 records saved to: {rank2_file}")
        
        # Show sample of clients with second insurance
        print(f"\nSample of clients with second insurance (first 15):")
        sample_rank2 = rank2_records.head(15)
        for _, record in sample_rank2.iterrows():
            print(f"  {record['CLIENT_ID']}: {record['DISASTER_INSURANCE_START_DATE']} - {record['RECORD_MONTH']} - {record['ACCT_TYPE']}")
    else:
        print("No rank 2 records found.")
    
    # Group by client and analyze patterns
    client_patterns = []
    for client_id in df['CLIENT_ID'].unique():
        client_records = df[df['CLIENT_ID'] == client_id].sort_values('DISASTER_INSURANCE_START_DATE')
        
        if len(client_records) > 1:
            # Calculate time difference between first and last insurance
            first_date = client_records.iloc[0]['DISASTER_INSURANCE_START_DATE']
            last_date = client_records.iloc[-1]['DISASTER_INSURANCE_START_DATE']
            days_between = (last_date - first_date).days
            
            client_patterns.append({
                'CLIENT_ID': client_id,
                'FIRST_INSURANCE': first_date.strftime('%Y-%m-%d'),
                'LAST_INSURANCE': last_date.strftime('%Y-%m-%d'),
                'DAYS_BETWEEN': days_between,
                'RECORD_COUNT': len(client_records)
            })
    
    if client_patterns:
        patterns_df = pd.DataFrame(client_patterns)
        patterns_df = patterns_df.sort_values('DAYS_BETWEEN', ascending=False)
        
        print(f"\nClients with multiple insurance records:")
        print(f"  - Total: {len(patterns_df)}")
        print(f"  - Average days between first and last: {patterns_df['DAYS_BETWEEN'].mean():.1f}")
        print(f"  - Maximum days between: {patterns_df['DAYS_BETWEEN'].max()}")
        print(f"  - Minimum days between: {patterns_df['DAYS_BETWEEN'].min()}")
        
        # Save patterns analysis
        patterns_file = 'insurance_patterns_analysis.csv'
        patterns_df.to_csv(patterns_file, index=False)
        print(f"\nInsurance patterns analysis saved to: {patterns_file}")
        
        # Show top 10 clients with longest time between insurance records
        print(f"\nTop 10 clients with longest time between insurance records:")
        print(patterns_df.head(10)[['CLIENT_ID', 'FIRST_INSURANCE', 'LAST_INSURANCE', 'DAYS_BETWEEN']].to_string(index=False))

def count_rank_distribution_by_month(df):
    """Count the distribution of ranks by month"""
    if df is None:
        return
    
    print(f"\n" + "="*60)
    print("RANK DISTRIBUTION BY MONTH ANALYSIS")
    print("="*60)
    
    # Create a pivot table showing rank distribution by month
    rank_month_pivot = df.pivot_table(
        index='RECORD_MONTH', 
        columns='RANK', 
        values='CLIENT_ID', 
        aggfunc='count',
        fill_value=0
    )
    
    # Rename columns for clarity
    rank_month_pivot.columns = [f'Rank_{col}' for col in rank_month_pivot.columns]
    
    # Add total column
    rank_month_pivot['Total_Records'] = rank_month_pivot.sum(axis=1)
    
    # Get all unique months and sort them chronologically
    all_months = df['RECORD_MONTH'].unique()
    print(f"Found months in data: {sorted(all_months)}")
    
    # Define chronological order for 2025 months
    month_order_2025 = ['Jan\'25', 'Feb\'25', 'Mar\'25', 'Apr\'25', 'May\'25', 'Jun\'25', 'Jul\'25', 'Aug\'25', 'Sep\'25', 'Oct\'25', 'Nov\'25', 'Dec\'25']
    
    # Filter to only include months that exist in the data and sort them
    existing_months = [m for m in month_order_2025 if m in all_months]
    
    # Add any other months that might exist (like 2024 months)
    other_months = [m for m in all_months if m not in month_order_2025]
    if other_months:
        print(f"Additional months found: {sorted(other_months)}")
        # Try to sort other months chronologically if they have a similar format
        try:
            other_months_sorted = sorted(other_months, key=lambda x: x.split('\'')[1] + x.split('\'')[0] if '\'' in x else x)
        except:
            other_months_sorted = sorted(other_months)
        existing_months.extend(other_months_sorted)
    
    # Reindex with the chronological order
    rank_month_pivot = rank_month_pivot.reindex(existing_months)
    
    print(f"\nRank distribution by month (chronological order):")
    print(rank_month_pivot.to_string())
    
    # Save the rank distribution analysis
    rank_dist_file = 'rank_distribution_by_month.csv'
    rank_month_pivot.to_csv(rank_dist_file)
    print(f"\nRank distribution by month saved to: {rank_dist_file}")
    
    # Show summary statistics
    print(f"\nSummary by month:")
    for month in existing_months:
        if month in rank_month_pivot.index:
            rank2_count = rank_month_pivot.loc[month, 'Rank_2'] if 'Rank_2' in rank_month_pivot.columns else 0
            total_count = rank_month_pivot.loc[month, 'Total_Records']
            print(f"  {month}: {total_count} total records, {rank2_count} second insurance records")
    
    return rank_month_pivot

if __name__ == "__main__":
    # Extract and rank the insurance dates
    ranked_df = extract_and_rank_insurance_dates()
    
    # Analyze patterns
    analyze_insurance_patterns(ranked_df)
    
    # Count rank distribution by month
    count_rank_distribution_by_month(ranked_df)
    
    print(f"\n" + "="*60)
    print("EXTRACTION AND ANALYSIS COMPLETE!")
    print("="*60)
