"""
Statistical Significance Analysis for Demographic Attributes
Shows which demographic features are statistically significant
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import chi2_contingency
import warnings
warnings.filterwarnings('ignore')

def load_data():
    """Load the main segments data"""
    print("Loading data from main_segments query...")
    df = pd.read_csv('/Users/fido_josephine/Documents/Work-AId/savings_models/main-segments-updated.csv')
    
    # Clean and prepare the data
    df['BEHAVIORAL_SEGMENT'] = df['BEHAVIORAL_SEGMENT'].str.lower()
    
    # Filter to main behavioral segments for analysis
    df = df[df['BEHAVIORAL_SEGMENT'].isin(['savers days active >=3, balance < 400 ', 'ultra_savers balance > 400', 'wallet_users monthly deposits and/or withdrawal frequency >=3'])]
    
    print(f"Loaded {len(df)} behavioral segment records")
    print(f"Behavioral segments: {df['BEHAVIORAL_SEGMENT'].value_counts().to_dict()}")
    
    return df

def prepare_demographic_data(df):
    """Prepare demographic data for analysis"""
    
    # Create age groups
    df['age_group'] = pd.cut(df['AGE'], 
                           bins=[0, 25, 35, 45, 55, 100], 
                           labels=['18-25', '26-35', '36-45', '46-55', '55+'],
                           include_lowest=True)
    
    return df

def calculate_statistical_significance(df):
    """Calculate statistical significance of each feature"""
    print("\nCalculating statistical significance...")
    
    results = []
    
    # Categorical features - Chi-square test
    categorical_features = ['age_group', 'INCOME_VALUE', 'REGION', 'EMPLOYMENT', 'MARITAL_STATUS', 'EDUCATION_LEVEL']
    
    for feature in categorical_features:
        if feature in df.columns:
            # Create contingency table
            contingency_table = pd.crosstab(df[feature], df['BEHAVIORAL_SEGMENT'])
            
            # Chi-square test
            chi2, p_value, dof, expected = chi2_contingency(contingency_table)
            
            results.append({
                'feature': feature,
                'test_type': 'Chi-square',
                'statistic': chi2,
                'p_value': p_value,
                'significant': p_value < 0.05,
                'effect_size': 'Cramér\'s V' if p_value < 0.05 else 'N/A'
            })
    
    # Convert to DataFrame
    significance_df = pd.DataFrame(results)
    significance_df = significance_df.sort_values('p_value')
    
    print("Statistical Significance Results:")
    print(significance_df)
    
    return significance_df

def create_significance_visualization(significance_df):
    """Create statistical significance visualization"""
    print("\nCreating statistical significance visualization...")
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    fig.suptitle('Statistical Significance Analysis\n(Chi-square Tests for Demographic Features)', 
                 fontsize=16, fontweight='bold')
    
    # 1. P-values visualization
    significant_features = significance_df[significance_df['significant'] == True]
    
    if len(significant_features) > 0:
        bars1 = ax1.barh(range(len(significant_features)), -np.log10(significant_features['p_value']), 
                        color='lightcoral', edgecolor='darkred', alpha=0.7)
        ax1.set_yticks(range(len(significant_features)))
        ax1.set_yticklabels(significant_features['feature'])
        ax1.set_xlabel('-log10(p-value)', fontsize=12, fontweight='bold')
        ax1.set_title('Statistical Significance\n(Features with p < 0.05)', fontsize=14, fontweight='bold')
        ax1.axvline(x=-np.log10(0.05), color='red', linestyle='--', alpha=0.7, label='p=0.05')
        ax1.legend()
        ax1.grid(axis='x', alpha=0.3)
        
        # Add value labels
        for i, bar in enumerate(bars1):
            width = bar.get_width()
            ax1.text(width + 0.1, bar.get_y() + bar.get_height()/2, 
                    f'p={significant_features.iloc[i]["p_value"]:.4f}', 
                    ha='left', va='center', fontsize=9, fontweight='bold')
    else:
        ax1.text(0.5, 0.5, 'No significant features\n(p < 0.05)', 
                ha='center', va='center', transform=ax1.transAxes, fontsize=12)
        ax1.set_title('Statistical Significance')
    
    # 2. Chi-square statistics
    bars2 = ax2.barh(range(len(significance_df)), significance_df['statistic'], 
                    color='lightblue', edgecolor='navy', alpha=0.7)
    ax2.set_yticks(range(len(significance_df)))
    ax2.set_yticklabels(significance_df['feature'])
    ax2.set_xlabel('Chi-square Statistic', fontsize=12, fontweight='bold')
    ax2.set_title('Chi-square Statistics\n(All Features)', fontsize=14, fontweight='bold')
    ax2.grid(axis='x', alpha=0.3)
    
    # Add significance indicators
    for i, (_, row) in enumerate(significance_df.iterrows()):
        if row['significant']:
            ax2.text(row['statistic'] + 0.5, i, '***', ha='left', va='center', 
                    fontsize=12, fontweight='bold', color='red')
    
    # Add value labels
    for i, bar in enumerate(bars2):
        width = bar.get_width()
        ax2.text(width + 0.5, bar.get_y() + bar.get_height()/2, 
                f'{width:.1f}', ha='left', va='center', fontsize=9, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('/Users/fido_josephine/Documents/Work-AId/savings_models/statistical_significance_analysis.png', 
                dpi=300, bbox_inches='tight')
    plt.show()
    
    return significance_df

def generate_insights(significance_df):
    """Generate insights from statistical significance analysis"""
    print("\n" + "="*80)
    print("STATISTICAL SIGNIFICANCE ANALYSIS INSIGHTS")
    print("="*80)
    
    # Statistical significance
    significant_features = significance_df[significance_df['significant'] == True]
    print(f"\nSTATISTICALLY SIGNIFICANT FEATURES (p < 0.05):")
    if len(significant_features) > 0:
        for i, (_, row) in enumerate(significant_features.iterrows(), 1):
            print(f"  {i}. {row['feature']} ({row['test_type']}): p = {row['p_value']:.4f}")
    else:
        print("  No features are statistically significant at p < 0.05")
    
    # All features ranked by significance
    print(f"\nALL FEATURES RANKED BY SIGNIFICANCE:")
    for i, (_, row) in enumerate(significance_df.iterrows(), 1):
        significance_level = "***" if row['significant'] else ""
        print(f"  {i}. {row['feature']}: p = {row['p_value']:.4f} {significance_level}")
    
    # Key insights
    print(f"\nKEY INSIGHTS:")
    print(f"  • Most significant feature: {significance_df.iloc[0]['feature']} (p = {significance_df.iloc[0]['p_value']:.4f})")
    print(f"  • Least significant feature: {significance_df.iloc[-1]['feature']} (p = {significance_df.iloc[-1]['p_value']:.4f})")
    print(f"  • Number of significant features: {len(significant_features)}")
    
    if len(significant_features) > 0:
        most_significant = significant_features.iloc[0]
        print(f"  • Most significant feature: {most_significant['feature']} (p = {most_significant['p_value']:.4f})")
    
    # Recommendations
    print(f"\nRECOMMENDATIONS:")
    if len(significant_features) > 0:
        print(f"  1. Prioritize statistically significant features for reliable segmentation")
        print(f"  2. Focus on {significant_features.iloc[0]['feature']} as the most significant predictor")
        print(f"  3. Use {significant_features.iloc[1]['feature']} as secondary significant predictor")
    else:
        print(f"  1. All features show similar statistical significance - use Random Forest importance for ranking")
        print(f"  2. Consider larger sample size or different statistical tests")
    print(f"  3. Test different combinations of features for optimal persona creation")

def main():
    """Main function to run statistical significance analysis"""
    print("Starting Statistical Significance Analysis...")
    print("Testing: Age Group, Income Range, Region, Employment, Marital Status, Education Level")
    
    # Load and prepare data
    df = load_data()
    df = prepare_demographic_data(df)
    
    # Calculate statistical significance
    significance_df = calculate_statistical_significance(df)
    
    # Create visualization
    create_significance_visualization(significance_df)
    
    # Generate insights
    generate_insights(significance_df)
    
    print("\n" + "="*80)
    print("STATISTICAL SIGNIFICANCE ANALYSIS COMPLETE")
    print("="*80)

if __name__ == "__main__":
    main()

