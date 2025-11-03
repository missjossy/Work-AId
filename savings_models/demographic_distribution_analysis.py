"""
Demographic Distribution Analysis
Shows distribution of each demographic attribute by behavioral segment
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

def create_demographic_distribution_visualization(df):
    """Create demographic distribution visualization"""
    print("\nCreating demographic distribution visualization...")
    
    # Define the demographic features to analyze
    demographic_features = ['GENDER', 'age_group', 'INCOME_VALUE', 'REGION', 'EMPLOYMENT', 'MARITAL_STATUS', 'EDUCATION_LEVEL']
    
    # Create a large figure with subplots
    fig, axes = plt.subplots(3, 3, figsize=(20, 16))
    fig.suptitle('Demographic Distribution by Behavioral Segment', fontsize=20, fontweight='bold')
    
    # Flatten axes for easier indexing
    axes = axes.flatten()
    
    for i, feature in enumerate(demographic_features):
        if i < len(axes):
            ax = axes[i]
            
            # Create contingency table
            contingency_table = pd.crosstab(df[feature], df['BEHAVIORAL_SEGMENT'])
            
            # Calculate percentages
            contingency_pct = contingency_table.div(contingency_table.sum(axis=1), axis=0) * 100
            
            # Create stacked bar chart
            contingency_pct.plot(kind='bar', stacked=True, ax=ax, 
                               color=['lightblue', 'darkblue', 'orange'], 
                               width=0.8, alpha=0.8)
            
            ax.set_title(f'{feature.replace("_", " ").title()}', fontsize=12, fontweight='bold')
            ax.set_xlabel('')
            ax.set_ylabel('Percentage', fontsize=10)
            ax.legend(title='Behavioral Segment', fontsize=8, title_fontsize=9)
            ax.tick_params(axis='x', rotation=45, labelsize=8)
            ax.grid(axis='y', alpha=0.3)
            
            # Add sample size annotations
            total_counts = contingency_table.sum(axis=1)
            for j, (idx, count) in enumerate(total_counts.items()):
                ax.text(j, 105, f'n={count}', ha='center', va='bottom', fontsize=8, fontweight='bold')
    
    # Hide unused subplots
    for i in range(len(demographic_features), len(axes)):
        axes[i].set_visible(False)
    
    plt.tight_layout()
    plt.savefig('/Users/fido_josephine/Documents/Work-AId/savings_models/demographic_distribution_analysis.png', 
                dpi=300, bbox_inches='tight')
    plt.show()

def create_heatmap_visualization(df):
    """Create heatmap visualization for demographic combinations"""
    print("\nCreating demographic combination heatmap...")
    
    # Create age groups
    df['age_group'] = pd.cut(df['AGE'], 
                           bins=[0, 25, 35, 45, 55, 100], 
                           labels=['18-25', '26-35', '36-45', '46-55', '55+'],
                           include_lowest=True)
    
    # Create combinations for top features
    top_features = ['GENDER', 'age_group', 'EMPLOYMENT', 'MARITAL_STATUS']
    
    # Create combination analysis
    combination_data = []
    for feature in top_features:
        if feature in df.columns:
            contingency_table = pd.crosstab(df[feature], df['BEHAVIORAL_SEGMENT'])
            contingency_pct = contingency_table.div(contingency_table.sum(axis=1), axis=0) * 100
            
            # Get top categories by count
            top_categories = contingency_table.sum(axis=1).nlargest(5).index
            
            for category in top_categories:
                if category in contingency_pct.index:
                    row_data = {
                        'Feature': feature,
                        'Category': str(category),
                        'Savers': contingency_pct.loc[category, 'savers days active >=3, balance < 400 '] if 'savers days active >=3, balance < 400 ' in contingency_pct.columns else 0,
                        'Ultra Savers': contingency_pct.loc[category, 'ultra_savers balance > 400'] if 'ultra_savers balance > 400' in contingency_pct.columns else 0,
                        'Wallet Users': contingency_pct.loc[category, 'wallet_users monthly deposits and/or withdrawal frequency >=3'] if 'wallet_users monthly deposits and/or withdrawal frequency >=3' in contingency_pct.columns else 0
                    }
                    combination_data.append(row_data)
    
    combination_df = pd.DataFrame(combination_data)
    
    # Create heatmap
    plt.figure(figsize=(12, 8))
    
    # Pivot for heatmap
    heatmap_data = combination_df.set_index(['Feature', 'Category'])[['Savers', 'Ultra Savers', 'Wallet Users']]
    
    # Create heatmap
    sns.heatmap(heatmap_data, annot=True, fmt='.1f', cmap='RdYlBu', 
                cbar_kws={'label': 'Percentage'}, linewidths=0.5)
    
    plt.title('Demographic Distribution Heatmap\n(Top Categories by Count)', fontsize=16, fontweight='bold')
    plt.xlabel('Behavioral Segment', fontsize=12, fontweight='bold')
    plt.ylabel('Feature - Category', fontsize=12, fontweight='bold')
    plt.xticks(rotation=45)
    plt.yticks(rotation=0)
    
    plt.tight_layout()
    plt.savefig('/Users/fido_josephine/Documents/Work-AId/savings_models/demographic_heatmap_analysis.png', 
                dpi=300, bbox_inches='tight')
    plt.show()

def generate_insights(df):
    """Generate insights from demographic distribution analysis"""
    print("\n" + "="*80)
    print("DEMOGRAPHIC DISTRIBUTION ANALYSIS INSIGHTS")
    print("="*80)
    
    # Segment sizes
    segment_sizes = df['BEHAVIORAL_SEGMENT'].value_counts()
    print(f"\nBehavioral Segment Distribution:")
    for segment, count in segment_sizes.items():
        pct = (count / len(df)) * 100
        print(f"  {segment}: {count:,} ({pct:.1f}%)")
    
    # Key demographic insights for each segment
    print(f"\nKey Demographic Patterns:")
    
    # Gender patterns
    gender_pct = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['GENDER'], normalize='index') * 100
    print(f"\nGender Distribution:")
    for segment in df['BEHAVIORAL_SEGMENT'].unique():
        male_pct = gender_pct.loc[segment, 'MALE'] if 'MALE' in gender_pct.columns else 0
        female_pct = gender_pct.loc[segment, 'FEMALE'] if 'FEMALE' in gender_pct.columns else 0
        print(f"  {segment}: {male_pct:.1f}% Male, {female_pct:.1f}% Female")
    
    # Age patterns
    age_stats = df.groupby('BEHAVIORAL_SEGMENT')['AGE'].agg(['mean', 'std']).round(1)
    print(f"\nAge Patterns (Mean ± Std):")
    for segment in age_stats.index:
        mean_age = age_stats.loc[segment, 'mean']
        std_age = age_stats.loc[segment, 'std']
        print(f"  {segment}: {mean_age:.1f} ± {std_age:.1f} years")
    
    # Employment patterns
    if 'EMPLOYMENT' in df.columns:
        employment_pct = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['EMPLOYMENT'], normalize='index') * 100
        print(f"\nTop Employment Categories by Segment:")
        for segment in df['BEHAVIORAL_SEGMENT'].unique():
            top_employment = employment_pct.loc[segment].nlargest(3)
            print(f"  {segment}:")
            for emp, pct in top_employment.items():
                print(f"    {emp}: {pct:.1f}%")
    
    # Marital status patterns
    if 'MARITAL_STATUS' in df.columns:
        marital_pct = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['MARITAL_STATUS'], normalize='index') * 100
        print(f"\nMarital Status Distribution:")
        for segment in df['BEHAVIORAL_SEGMENT'].unique():
            top_marital = marital_pct.loc[segment].nlargest(3)
            print(f"  {segment}:")
            for status, pct in top_marital.items():
                print(f"    {status}: {pct:.1f}%")
    
    # Education patterns
    if 'EDUCATION_LEVEL' in df.columns:
        education_pct = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['EDUCATION_LEVEL'], normalize='index') * 100
        print(f"\nEducation Level Distribution:")
        for segment in df['BEHAVIORAL_SEGMENT'].unique():
            top_education = education_pct.loc[segment].nlargest(3)
            print(f"  {segment}:")
            for edu, pct in top_education.items():
                print(f"    {edu}: {pct:.1f}%")

def main():
    """Main function to run demographic distribution analysis"""
    print("Starting Demographic Distribution Analysis...")
    print("Analyzing: Gender, Age, Income, Region, Employment, Marital Status, Education Level")
    
    # Load and prepare data
    df = load_data()
    df = prepare_demographic_data(df)
    
    # Create visualizations
    create_demographic_distribution_visualization(df)
    create_heatmap_visualization(df)
    
    # Generate insights
    generate_insights(df)
    
    print("\n" + "="*80)
    print("DEMOGRAPHIC DISTRIBUTION ANALYSIS COMPLETE")
    print("="*80)

if __name__ == "__main__":
    main()

