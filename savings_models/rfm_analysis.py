"""
RFM Analysis for Savings Personas
Adapted for savings behavior using Recency, Frequency, and Monetary metrics
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

def load_data():
    """Load the main segments data"""
    print("Loading data from main_segments query...")
    df = pd.read_csv('/Users/fido_josephine/Documents/Work-AId/savings_models/main_segemnts_data.csv')
    
    # Clean and prepare the data
    df['BEHAVIORAL_SEGMENT'] = df['BEHAVIORAL_SEGMENT'].str.lower()
    
    print(f"Loaded {len(df)} records")
    print(f"Behavioral segments: {df['BEHAVIORAL_SEGMENT'].value_counts().to_dict()}")
    
    return df

def calculate_rfm_metrics(df):
    """
    Calculate RFM metrics for savings behavior
    
    Recency: Days since last deposit (using days_active as proxy)
    Frequency: Number of deposits per month (average)
    Monetary: Average deposit amount and current balance
    """
    
    # Calculate Recency (using days_active as proxy for recency)
    # Lower days_active means more recent activity
    df['recency'] = df['DAYS_ACTIVE']
    
    # Calculate Frequency (deposits per month)
    df['frequency'] = df['MONTHLY_DEPOSIT_FREQUENCY']
    
    # Calculate Monetary (average deposit amount and current balance)
    df['monetary_deposits'] = df['TOTAL_DEPOSITS_AMOUNT'] / df['DEPOSITS'].replace(0, 1)
    df['monetary_balance'] = df['LAST_BALANCE']
    
    # Create composite monetary score (weighted average)
    df['monetary_score'] = (df['monetary_deposits'] * 0.3 + df['monetary_balance'] * 0.7)
    
    return df

def create_rfm_segments(df):
    """
    Create RFM segments based on quartiles
    """
    
    # Create RFM scores (1-4, where 4 is best)
    # Handle duplicate values by using 'duplicates' parameter
    try:
        df['recency_score'] = pd.qcut(df['recency'], 4, labels=[4, 3, 2, 1], duplicates='drop')  # Lower recency is better
    except ValueError:
        # If still having issues, use rank-based scoring
        df['recency_score'] = pd.qcut(df['recency'].rank(method='first'), 4, labels=[4, 3, 2, 1], duplicates='drop')
    
    try:
        df['frequency_score'] = pd.qcut(df['frequency'], 4, labels=[1, 2, 3, 4], duplicates='drop')  # Higher frequency is better
    except ValueError:
        df['frequency_score'] = pd.qcut(df['frequency'].rank(method='first'), 4, labels=[1, 2, 3, 4], duplicates='drop')
    
    try:
        df['monetary_score'] = pd.qcut(df['monetary_score'], 4, labels=[1, 2, 3, 4], duplicates='drop')  # Higher monetary is better
    except ValueError:
        df['monetary_score'] = pd.qcut(df['monetary_score'].rank(method='first'), 4, labels=[1, 2, 3, 4], duplicates='drop')
    
    # Convert to numeric
    df['recency_score'] = df['recency_score'].astype(int)
    df['frequency_score'] = df['frequency_score'].astype(int)
    df['monetary_score'] = df['monetary_score'].astype(int)
    
    # Create RFM segments
    def get_rfm_segment(row):
        r, f, m = row['recency_score'], row['frequency_score'], row['monetary_score']
        
        if r >= 3 and f >= 3 and m >= 3:
            return 'Champions'
        elif r >= 2 and f >= 3 and m >= 2:
            return 'Loyal Customers'
        elif r >= 3 and f >= 2 and m >= 2:
            return 'Potential Loyalists'
        elif r >= 3 and f >= 1 and m >= 1:
            return 'New Customers'
        elif r >= 2 and f >= 2 and m >= 2:
            return 'Promising'
        elif r >= 2 and f >= 1 and m >= 1:
            return 'Need Attention'
        elif r >= 1 and f >= 2 and m >= 2:
            return 'About to Sleep'
        elif r >= 1 and f >= 1 and m >= 1:
            return 'At Risk'
        else:
            return 'Cannot Lose Them'
    
    df['rfm_segment'] = df.apply(get_rfm_segment, axis=1)
    
    return df

def analyze_personas_by_rfm(df):
    """
    Analyze how existing personas (ultra_savers, savers, wallet_users) 
    map to RFM segments
    """
    
    # Create cross-tabulation
    persona_rfm = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['rfm_segment'], margins=True)
    
    # Calculate percentages
    persona_rfm_pct = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['rfm_segment'], normalize='index') * 100
    
    return persona_rfm, persona_rfm_pct

def create_rfm_heatmap(df):
    """
    Create heatmap showing RFM distribution by persona
    """
    
    # Pivot table for heatmap
    rfm_pivot = df.groupby(['BEHAVIORAL_SEGMENT', 'rfm_segment']).size().unstack(fill_value=0)
    
    plt.figure(figsize=(12, 8))
    sns.heatmap(rfm_pivot, annot=True, fmt='d', cmap='YlOrRd')
    plt.title('RFM Segments by Behavioral Persona')
    plt.xlabel('RFM Segment')
    plt.ylabel('Behavioral Segment')
    plt.tight_layout()
    plt.show()

def calculate_persona_rfm_stats(df):
    """
    Calculate RFM statistics for each persona
    """
    
    stats = df.groupby('BEHAVIORAL_SEGMENT').agg({
        'recency': ['mean', 'std'],
        'frequency': ['mean', 'std'],
        'monetary_score': ['mean', 'std'],
        'DEPOSITS': 'mean',
        'WITHDRAWALS': 'mean',
        'LAST_BALANCE': 'mean'
    }).round(2)
    
    return stats

def main():
    """
    Main function to run RFM analysis
    """
    print("=== RFM Analysis for Savings Personas ===\n")
    
    # Load data (replace with actual data loading)
    print("1. Loading data...")
    df = load_data()
    
    if df is None:
        print("Please load your data from the main_segments query first.")
        print("Expected columns: client_id, behavioral_segment, total_deposits_count, total_deposits_amount, last_balance, last_transaction_date, monthly_deposit_frequency")
        return
    
    print("2. Calculating RFM metrics...")
    df = calculate_rfm_metrics(df)
    
    print("3. Creating RFM segments...")
    df = create_rfm_segments(df)
    
    print("4. Analyzing personas by RFM...")
    persona_rfm, persona_rfm_pct = analyze_personas_by_rfm(df)
    
    print("\n=== RFM Distribution by Persona ===")
    print(persona_rfm)
    
    print("\n=== RFM Distribution by Persona (%) ===")
    print(persona_rfm_pct.round(1))
    
    print("\n=== RFM Statistics by Persona ===")
    stats = calculate_persona_rfm_stats(df)
    print(stats)
    
    print("\n5. Creating visualizations...")
    create_rfm_heatmap(df)
    
    print("\n=== Key Insights ===")
    print("• Champions: High recency, frequency, and monetary value")
    print("• Loyal Customers: Regular savers with good monetary value")
    print("• Potential Loyalists: Recent savers with good frequency")
    print("• At Risk: Low recency but good historical behavior")
    print("• Cannot Lose Them: Low across all metrics")
    
    return df

if __name__ == "__main__":
    main()
