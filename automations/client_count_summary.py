import pandas as pd

def generate_client_count_summary():
    """Generate a summary of client IDs and their appearance counts"""
    
    # Load the duplicate insurance records
    try:
        df = pd.read_csv('duplicate_insurance_records.csv')
        print("Loading duplicate insurance records...")
        print(f"Total records: {len(df)}")
        
        # Count occurrences of each client
        client_counts = df['CLIENT_ID'].value_counts().reset_index()
        client_counts.columns = ['CLIENT_ID', 'APPEARANCE_COUNT']
        
        # Sort by count (highest first)
        client_counts = client_counts.sort_values('APPEARANCE_COUNT', ascending=False)
        
        print(f"\nFound {len(client_counts)} unique clients with multiple records")
        print("\n" + "="*60)
        print("CLIENT ID AND APPEARANCE COUNT SUMMARY")
        print("="*60)
        
        # Display the summary
        print(client_counts.to_string(index=False))
        
        # Save to CSV
        output_file = 'client_appearance_counts.csv'
        client_counts.to_csv(output_file, index=False)
        print(f"\nSummary saved to: {output_file}")
        
        # Show some statistics
        print(f"\nStatistics:")
        print(f"  - Clients with 2 appearances: {len(client_counts[client_counts['APPEARANCE_COUNT'] == 2])}")
        print(f"  - Clients with 3+ appearances: {len(client_counts[client_counts['APPEARANCE_COUNT'] >= 3])}")
        print(f"  - Highest appearance count: {client_counts['APPEARANCE_COUNT'].max()}")
        print(f"  - Average appearance count: {client_counts['APPEARANCE_COUNT'].mean():.2f}")
        
    except FileNotFoundError:
        print("Error: duplicate_insurance_records.csv not found. Please run the main analysis first.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    generate_client_count_summary()
