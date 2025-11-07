#!/usr/bin/env python3
"""
Script to find transactions in the Excel file that are not in the CSV disbursements file.
"""

import pandas as pd
from datetime import datetime

# File paths
csv_file = 'Disbursements_-_UGANDA_Finance_Department_2025_11_06.csv'
excel_file = 'Transactions-fidoug-stellan-2025-11-06T10_29_04.894_03_00.xlsx'
output_file = 'missing_transactions.csv'

print("Loading files...")
# Read CSV file
csv_df = pd.read_csv(csv_file)
print(f"CSV file loaded: {len(csv_df)} records")

# Read Excel file
excel_df = pd.read_excel(excel_file)
print(f"Excel file loaded: {len(excel_df)} records")

# Clean and prepare data for matching
# Convert dates to datetime for comparison
csv_df['DISBURSMENT_DATE'] = pd.to_datetime(csv_df['DISBURSMENT_DATE'], errors='coerce')
excel_df['Value Date (Entry Date)'] = pd.to_datetime(excel_df['Value Date (Entry Date)'], errors='coerce')

# Normalize amounts (remove decimals if needed, convert to float)
csv_df['DISBURSED_AMOUNT'] = csv_df['DISBURSED_AMOUNT'].astype(float)
excel_df['Amount'] = excel_df['Amount'].astype(float)

# Match on Account ID (Excel) vs LOAN_ID (CSV)
# These are the primary identifiers that should match
csv_loan_ids = set(csv_df['LOAN_ID'].dropna().astype(str))
excel_account_ids = set(excel_df['Account ID'].dropna().astype(str))

# Find transactions in Excel that don't have matching LOAN_ID in CSV
missing_transactions = excel_df[~excel_df['Account ID'].astype(str).isin(csv_loan_ids)]

print(f"\nTransactions in Excel not found in CSV (by Account ID/LOAN_ID): {len(missing_transactions)}")
print(f"Total transactions in Excel: {len(excel_df)}")
print(f"Total transactions in CSV: {len(csv_df)}")
print(f"Matched transactions: {len(excel_df) - len(missing_transactions)}")

print(f"\nTotal unique missing transactions: {len(missing_transactions)}")

# Save to CSV
if len(missing_transactions) > 0:
    missing_transactions.to_csv(output_file, index=False)
    print(f"\nMissing transactions saved to: {output_file}")
    print(f"\nFirst few missing transactions:")
    print(missing_transactions.head(10).to_string())
    
    # Summary statistics
    print(f"\n--- Summary ---")
    print(f"Total missing transactions: {len(missing_transactions)}")
    print(f"Total amount of missing transactions: {missing_transactions['Amount'].sum():,.0f}")
    print(f"Date range: {missing_transactions['Value Date (Entry Date)'].min()} to {missing_transactions['Value Date (Entry Date)'].max()}")
    print(f"\nChannels:")
    print(missing_transactions['Channel'].value_counts())
else:
    print("\nNo missing transactions found!")

