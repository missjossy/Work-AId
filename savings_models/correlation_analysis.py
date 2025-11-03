"""
Correlation Analysis for Savings Personas
Analyze relationships between demographics and savings behavior
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import pearsonr, spearmanr, chi2_contingency
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

def prepare_demographic_data(df):
    """
    Prepare demographic data for correlation analysis
    """
    
    # Create age groups
    df['age_group'] = pd.cut(df['AGE'], 
                           bins=[0, 25, 35, 45, 55, 100], 
                           labels=['18-25', '26-35', '36-45', '46-55', '55+'])
    
    # Create Fido score groups
    df['fido_score_group'] = pd.cut(df['FIDO_SCORE_AT_SIGNUP'], 
                                   bins=[0, 250, 400, 600, 1000], 
                                   labels=['Low (0-250)', 'Medium (250-400)', 'High (400-600)', 'Very High (600+)'])
    
    # Create income groups (assuming income_value is available)
    if 'INCOME_VALUE' in df.columns:
        df['income_group'] = pd.cut(df['INCOME_VALUE'], 
                                   bins=[0, 1000, 3000, 5000, 10000, float('inf')], 
                                   labels=['Very Low', 'Low', 'Medium', 'High', 'Very High'])
    
    # Encode categorical variables
    df['gender_encoded'] = df['GENDER'].map({'MALE': 1, 'FEMALE': 0})
    df['region_encoded'] = pd.Categorical(df['REGION']).codes
    
    return df

def calculate_savings_metrics(df):
    """
    Calculate key savings behavior metrics
    """
    
    # Primary savings metrics
    df['savings_consistency'] = df['DEPOSITS'] / df['DAYS_ACTIVE'].replace(0, 1)
    df['withdrawal_ratio'] = df['WITHDRAWALS'] / df['DEPOSITS'].replace(0, 1)
    df['balance_growth_rate'] = df['LAST_BALANCE'] / df['DAYS_ACTIVE'].replace(0, 1)
    df['deposit_frequency'] = df['MONTHLY_DEPOSIT_FREQUENCY']
    df['withdrawal_frequency'] = df['MONTHLY_WITHDRAWAL_FREQUENCY']
    
    # Create composite "savings quality" score
    df['savings_quality_score'] = (
        df['LAST_BALANCE'] * 0.3 +
        df['savings_consistency'] * 0.2 +
        (1 - df['withdrawal_ratio']) * 0.2 +
        df['deposit_frequency'] * 0.3
    )
    
    return df

def demographic_correlation_analysis(df):
    """
    Analyze correlations between demographics and savings behavior
    """
    
    # Select numeric columns for correlation
    numeric_cols = [
        'AGE', 'FIDO_SCORE_AT_SIGNUP', 'INCOME_VALUE',
        'savings_quality_score', 'LAST_BALANCE', 'DEPOSITS',
        'WITHDRAWALS', 'deposit_frequency', 'withdrawal_frequency',
        'savings_consistency', 'withdrawal_ratio', 'balance_growth_rate'
    ]
    
    # Filter to available columns
    available_cols = [col for col in numeric_cols if col in df.columns]
    correlation_data = df[available_cols].corr()
    
    return correlation_data

def persona_demographic_analysis(df):
    """
    Analyze demographic distributions by persona
    """
    
    # Gender distribution by persona
    gender_persona = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['GENDER'], normalize='index') * 100
    
    # Age group distribution by persona
    age_persona = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['age_group'], normalize='index') * 100
    
    # Fido score distribution by persona
    fido_persona = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['fido_score_group'], normalize='index') * 100
    
    # Income distribution by persona (if available)
    income_persona = None
    if 'income_group' in df.columns:
        income_persona = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['income_group'], normalize='index') * 100
    
    # Region distribution by persona
    region_persona = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['REGION'], normalize='index') * 100
    
    return {
        'gender': gender_persona,
        'age': age_persona,
        'fido_score': fido_persona,
        'income': income_persona,
        'region': region_persona
    }

def statistical_tests(df):
    """
    Perform statistical tests to validate relationships
    """
    
    results = {}
    
    # Chi-square test for categorical variables
    categorical_tests = [
        ('BEHAVIORAL_SEGMENT', 'GENDER'),
        ('BEHAVIORAL_SEGMENT', 'age_group'),
        ('BEHAVIORAL_SEGMENT', 'fido_score_group'),
        ('BEHAVIORAL_SEGMENT', 'REGION')
    ]
    
    for var1, var2 in categorical_tests:
        if var1 in df.columns and var2 in df.columns:
            contingency_table = pd.crosstab(df[var1], df[var2])
            chi2, p_value, dof, expected = chi2_contingency(contingency_table)
            results[f'{var1}_vs_{var2}'] = {
                'chi2': chi2,
                'p_value': p_value,
                'significant': p_value < 0.05
            }
    
    # Correlation tests for numeric variables
    numeric_vars = ['AGE', 'FIDO_SCORE_AT_SIGNUP', 'INCOME_VALUE']
    target_var = 'savings_quality_score'
    
    for var in numeric_vars:
        if var in df.columns and target_var in df.columns:
            # Pearson correlation
            pearson_r, pearson_p = pearsonr(df[var].dropna(), df[target_var].dropna())
            # Spearman correlation
            spearman_r, spearman_p = spearmanr(df[var].dropna(), df[target_var].dropna())
            
            results[f'{var}_correlation'] = {
                'pearson_r': pearson_r,
                'pearson_p': pearson_p,
                'spearman_r': spearman_r,
                'spearman_p': spearman_p,
                'pearson_significant': pearson_p < 0.05,
                'spearman_significant': spearman_p < 0.05
            }
    
    return results

def create_visualizations(df, persona_demos):
    """
    Create visualizations for the analysis
    """
    
    # Set up the plotting style
    plt.style.use('default')
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    
    # 1. Gender distribution by persona
    persona_demos['gender'].plot(kind='bar', ax=axes[0,0], stacked=True)
    axes[0,0].set_title('Gender Distribution by Persona')
    axes[0,0].set_xlabel('Persona')
    axes[0,0].set_ylabel('Percentage')
    axes[0,0].legend(title='Gender')
    axes[0,0].tick_params(axis='x', rotation=45)
    
    # 2. Age group distribution by persona
    persona_demos['age'].plot(kind='bar', ax=axes[0,1], stacked=True)
    axes[0,1].set_title('Age Group Distribution by Persona')
    axes[0,1].set_xlabel('Persona')
    axes[0,1].set_ylabel('Percentage')
    axes[0,1].legend(title='Age Group')
    axes[0,1].tick_params(axis='x', rotation=45)
    
    # 3. Fido score distribution by persona
    persona_demos['fido_score'].plot(kind='bar', ax=axes[0,2], stacked=True)
    axes[0,2].set_title('Fido Score Distribution by Persona')
    axes[0,2].set_xlabel('Persona')
    axes[0,2].set_ylabel('Percentage')
    axes[0,2].legend(title='Fido Score Group')
    axes[0,2].tick_params(axis='x', rotation=45)
    
    # 4. Income distribution by persona (if available)
    if persona_demos['income'] is not None:
        persona_demos['income'].plot(kind='bar', ax=axes[1,0], stacked=True)
        axes[1,0].set_title('Income Distribution by Persona')
        axes[1,0].set_xlabel('Persona')
        axes[1,0].set_ylabel('Percentage')
        axes[1,0].legend(title='Income Group')
        axes[1,0].tick_params(axis='x', rotation=45)
    else:
        axes[1,0].text(0.5, 0.5, 'Income data not available', ha='center', va='center')
        axes[1,0].set_title('Income Distribution by Persona')
    
    # 5. Region distribution by persona
    persona_demos['region'].plot(kind='bar', ax=axes[1,1], stacked=True)
    axes[1,1].set_title('Region Distribution by Persona')
    axes[1,1].set_xlabel('Persona')
    axes[1,1].set_ylabel('Percentage')
    axes[1,1].legend(title='Region')
    axes[1,1].tick_params(axis='x', rotation=45)
    
    # 6. Savings quality score by persona
    df.boxplot(column='savings_quality_score', by='BEHAVIORAL_SEGMENT', ax=axes[1,2])
    axes[1,2].set_title('Savings Quality Score by Persona')
    axes[1,2].set_xlabel('Persona')
    axes[1,2].set_ylabel('Savings Quality Score')
    
    plt.tight_layout()
    plt.show()

def main():
    """
    Main function to run correlation analysis
    """
    print("=== Correlation Analysis for Savings Personas ===\n")
    
    # Load data (replace with actual data loading)
    print("1. Loading data...")
    df = load_data()
    
    if df is None:
        print("Please load your data from the main_segments query first.")
        print("Expected columns: client_id, behavioral_segment, age, gender, fido_score_at_signup, income_value, region, total_deposits_count, total_deposits_amount, last_balance, monthly_deposit_frequency, monthly_withdrawal_frequency, days_active")
        return
    
    print("2. Preparing demographic data...")
    df = prepare_demographic_data(df)
    
    print("3. Calculating savings metrics...")
    df = calculate_savings_metrics(df)
    
    print("4. Running correlation analysis...")
    correlation_matrix = demographic_correlation_analysis(df)
    
    print("\n=== Correlation Matrix ===")
    print(correlation_matrix.round(3))
    
    print("5. Analyzing persona demographics...")
    persona_demos = persona_demographic_analysis(df)
    
    print("\n=== Gender Distribution by Persona (%) ===")
    print(persona_demos['gender'].round(1))
    
    print("\n=== Age Group Distribution by Persona (%) ===")
    print(persona_demos['age'].round(1))
    
    print("\n=== Fido Score Distribution by Persona (%) ===")
    print(persona_demos['fido_score'].round(1))
    
    print("6. Running statistical tests...")
    test_results = statistical_tests(df)
    
    print("\n=== Statistical Test Results ===")
    for test_name, results in test_results.items():
        print(f"\n{test_name}:")
        for key, value in results.items():
            print(f"  {key}: {value}")
    
    print("7. Creating visualizations...")
    create_visualizations(df, persona_demos)
    
    print("\n=== Key Insights ===")
    print("• Look for strong correlations (|r| > 0.3) between demographics and savings behavior")
    print("• Check p-values < 0.05 for statistical significance")
    print("• Identify which demographic groups are over-represented in 'ultra_savers'")
    print("• Compare 'wallet_users' vs 'savers' demographic patterns")
    
    return df

if __name__ == "__main__":
    main()
