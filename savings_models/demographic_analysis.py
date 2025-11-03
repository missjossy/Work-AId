"""
Demographic Analysis for Savings Behavioral Segments
Analyzes gender, age, fido score, income, and region distributions for different saver types
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
    
    # Filter to savers, ultra_savers, and wallet_users for analysis
    df = df[df['BEHAVIORAL_SEGMENT'].isin(['savers days active >=3, balance < 400 ', 'ultra_savers balance > 400', 'wallet_users monthly deposits and/or withdrawal frequency >=3'])]
    
    print(f"Loaded {len(df)} saver behavior records")
    print(f"Behavioral segments: {df['BEHAVIORAL_SEGMENT'].value_counts().to_dict()}")
    
    return df

def prepare_demographic_data(df):
    """Prepare demographic data for analysis"""
    
    # Create age groups
    df['age_group'] = pd.cut(df['AGE'], 
                           bins=[0, 25, 35, 45, 55, 100], 
                           labels=['18-25', '26-35', '36-45', '46-55', '55+'],
                           include_lowest=True)
    
    # Create Fido score groups
    df['fido_score_group'] = pd.cut(df['FIDO_SCORE_AT_SIGNUP'], 
                                   bins=[0, 250, 400, 600, 1000], 
                                   labels=['Low (0-250)', 'Medium (250-400)', 'High (400-600)', 'Very High (600+)'],
                                   include_lowest=True)
    
    # Handle income groups - convert string ranges to numeric values
    def income_to_numeric(income_str):
        if pd.isna(income_str):
            return np.nan
        elif income_str == 'Below 350 GHS':
            return 175  # midpoint
        elif income_str == '351 GHS - 700 GHS':
            return 525  # midpoint
        elif income_str == '701 GHS - 1000 GHS':
            return 850  # midpoint
        elif income_str == '1001 GHS - 1400 GHS':
            return 1200  # midpoint
        elif income_str == '1401 GHS - 1800 GHS':
            return 1600  # midpoint
        elif income_str == 'Above 1800 GHS':
            return 2000  # estimated
        else:
            return np.nan
    
    df['INCOME_VALUE_NUMERIC'] = df['INCOME_VALUE'].apply(income_to_numeric)
    
    # Create income groups with 'Unknown' category included
    income_mask = df['INCOME_VALUE_NUMERIC'].notna()
    
    # Initialize income_group column with 'Unknown'
    df['income_group'] = 'Unknown'
    
    # Create income groups only for non-null values
    if income_mask.any():
        df.loc[income_mask, 'income_group'] = pd.cut(df.loc[income_mask, 'INCOME_VALUE_NUMERIC'], 
                                                   bins=[0, 1000, 3000, 5000, 10000, float('inf')], 
                                                   labels=['Very Low', 'Low', 'Medium', 'High', 'Very High'],
                                                   include_lowest=True).astype(str)
    
    return df

def analyze_gender_distribution(df):
    """Analyze gender distribution by behavioral segment"""
    print("\n=== GENDER DISTRIBUTION BY SAVER TYPE ===")
    
    # Count and percentage
    gender_counts = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['GENDER'], margins=True)
    gender_pct = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['GENDER'], normalize='index') * 100
    
    print("\nCounts:")
    print(gender_counts)
    print("\nPercentages:")
    print(gender_pct.round(1))
    
    # Chi-square test
    contingency_table = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['GENDER'])
    chi2, p_value, dof, expected = chi2_contingency(contingency_table)
    print(f"\nChi-square test: χ² = {chi2:.3f}, p-value = {p_value:.3f}")
    print(f"Significant relationship: {'Yes' if p_value < 0.05 else 'No'}")
    
    return gender_counts, gender_pct

def analyze_age_distribution(df):
    """Analyze age distribution by behavioral segment"""
    print("\n=== AGE DISTRIBUTION BY SAVER TYPE ===")
    
    # Count and percentage
    age_counts = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['age_group'], margins=True)
    age_pct = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['age_group'], normalize='index') * 100
    
    print("\nCounts:")
    print(age_counts)
    print("\nPercentages:")
    print(age_pct.round(1))
    
    # Chi-square test
    contingency_table = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['age_group'])
    chi2, p_value, dof, expected = chi2_contingency(contingency_table)
    print(f"\nChi-square test: χ² = {chi2:.3f}, p-value = {p_value:.3f}")
    print(f"Significant relationship: {'Yes' if p_value < 0.05 else 'No'}")
    
    # Age statistics by segment
    print("\nAge Statistics by Segment:")
    age_stats = df.groupby('BEHAVIORAL_SEGMENT')['AGE'].agg(['count', 'mean', 'std', 'min', 'max']).round(1)
    print(age_stats)
    
    return age_counts, age_pct, age_stats

def analyze_fido_score_distribution(df):
    """Analyze Fido score distribution by behavioral segment"""
    print("\n=== FIDO SCORE DISTRIBUTION BY SAVER TYPE ===")
    
    # Count and percentage
    fido_counts = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['fido_score_group'], margins=True)
    fido_pct = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['fido_score_group'], normalize='index') * 100
    
    print("\nCounts:")
    print(fido_counts)
    print("\nPercentages:")
    print(fido_pct.round(1))
    
    # Chi-square test
    contingency_table = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['fido_score_group'])
    chi2, p_value, dof, expected = chi2_contingency(contingency_table)
    print(f"\nChi-square test: χ² = {chi2:.3f}, p-value = {p_value:.3f}")
    print(f"Significant relationship: {'Yes' if p_value < 0.05 else 'No'}")
    
    # Fido score statistics by segment
    print("\nFido Score Statistics by Segment:")
    fido_stats = df.groupby('BEHAVIORAL_SEGMENT')['FIDO_SCORE_AT_SIGNUP'].agg(['count', 'mean', 'std', 'min', 'max']).round(1)
    print(fido_stats)
    
    return fido_counts, fido_pct, fido_stats

def analyze_income_distribution(df):
    """Analyze income distribution by behavioral segment"""
    print("\n=== INCOME DISTRIBUTION BY SAVER TYPE ===")
    
    # Count and percentage
    income_counts = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['income_group'], margins=True)
    income_pct = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['income_group'], normalize='index') * 100
    
    print("\nCounts:")
    print(income_counts)
    print("\nPercentages:")
    print(income_pct.round(1))
    
    # Chi-square test
    contingency_table = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['income_group'])
    chi2, p_value, dof, expected = chi2_contingency(contingency_table)
    print(f"\nChi-square test: χ² = {chi2:.3f}, p-value = {p_value:.3f}")
    print(f"Significant relationship: {'Yes' if p_value < 0.05 else 'No'}")
    
    # Income statistics by segment (only for numeric values)
    print("\nIncome Statistics by Segment (Numeric values only):")
    income_numeric = df[df['INCOME_VALUE_NUMERIC'].notna()]
    if len(income_numeric) > 0:
        income_stats = income_numeric.groupby('BEHAVIORAL_SEGMENT')['INCOME_VALUE_NUMERIC'].agg(['count', 'mean', 'std', 'min', 'max']).round(1)
        print(income_stats)
    else:
        print("No numeric income data available")
        income_stats = None
    
    return income_counts, income_pct, income_stats

def analyze_region_distribution(df):
    """Analyze region distribution by behavioral segment"""
    print("\n=== REGION DISTRIBUTION BY SAVER TYPE ===")
    
    # Count and percentage
    region_counts = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['REGION'], margins=True)
    region_pct = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['REGION'], normalize='index') * 100
    
    print("\nCounts:")
    print(region_counts)
    print("\nPercentages:")
    print(region_pct.round(1))
    
    # Chi-square test
    contingency_table = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['REGION'])
    chi2, p_value, dof, expected = chi2_contingency(contingency_table)
    print(f"\nChi-square test: χ² = {chi2:.3f}, p-value = {p_value:.3f}")
    print(f"Significant relationship: {'Yes' if p_value < 0.05 else 'No'}")
    
    return region_counts, region_pct

def analyze_employment_distribution(df):
    """Analyze employment distribution by behavioral segment"""
    print("\n=== EMPLOYMENT DISTRIBUTION BY SAVER TYPE ===")
    
    # Count and percentage
    employment_counts = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['EMPLOYMENT'], margins=True)
    employment_pct = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['EMPLOYMENT'], normalize='index') * 100
    
    print("\nCounts:")
    print(employment_counts)
    print("\nPercentages:")
    print(employment_pct.round(1))
    
    # Chi-square test
    contingency_table = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['EMPLOYMENT'])
    chi2, p_value, dof, expected = chi2_contingency(contingency_table)
    print(f"\nChi-square test: χ² = {chi2:.3f}, p-value = {p_value:.3f}")
    print(f"Significant relationship: {'Yes' if p_value < 0.05 else 'No'}")
    
    return employment_counts, employment_pct

def analyze_marital_status_distribution(df):
    """Analyze marital status distribution by behavioral segment"""
    print("\n=== MARITAL STATUS DISTRIBUTION BY SAVER TYPE ===")
    
    # Count and percentage
    marital_counts = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['MARITAL_STATUS'], margins=True)
    marital_pct = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['MARITAL_STATUS'], normalize='index') * 100
    
    print("\nCounts:")
    print(marital_counts)
    print("\nPercentages:")
    print(marital_pct.round(1))
    
    # Chi-square test
    contingency_table = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['MARITAL_STATUS'])
    chi2, p_value, dof, expected = chi2_contingency(contingency_table)
    print(f"\nChi-square test: χ² = {chi2:.3f}, p-value = {p_value:.3f}")
    print(f"Significant relationship: {'Yes' if p_value < 0.05 else 'No'}")
    
    return marital_counts, marital_pct

def analyze_education_distribution(df):
    """Analyze education level distribution by behavioral segment"""
    print("\n=== EDUCATION LEVEL DISTRIBUTION BY SAVER TYPE ===")
    
    # Count and percentage
    education_counts = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['EDUCATION_LEVEL'], margins=True)
    education_pct = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['EDUCATION_LEVEL'], normalize='index') * 100
    
    print("\nCounts:")
    print(education_counts)
    print("\nPercentages:")
    print(education_pct.round(1))
    
    # Chi-square test
    contingency_table = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['EDUCATION_LEVEL'])
    chi2, p_value, dof, expected = chi2_contingency(contingency_table)
    print(f"\nChi-square test: χ² = {chi2:.3f}, p-value = {p_value:.3f}")
    print(f"Significant relationship: {'Yes' if p_value < 0.05 else 'No'}")
    
    return education_counts, education_pct

def create_combination_analysis(df):
    """Create combination analysis of demographic factors"""
    
    # Create combinations
    df['demographic_combination'] = (
        df['GENDER'].fillna('Unknown') + ' - ' + 
        df['age_group'].astype(str) + ' - ' + 
        df['INCOME_VALUE'].fillna('Unknown') + ' - ' + 
        df['fido_score_group'].astype(str) + ' - ' +
        df['EMPLOYMENT'].fillna('Unknown') + ' - ' +
        df['MARITAL_STATUS'].fillna('Unknown') + ' - ' +
        df['EDUCATION_LEVEL'].fillna('Unknown')
    )
    
    # Get combination counts and proportions
    combination_analysis = df.groupby(['demographic_combination', 'BEHAVIORAL_SEGMENT']).size().unstack(fill_value=0)
    combination_totals = combination_analysis.sum(axis=1)
    combination_proportions = combination_analysis.div(combination_totals, axis=0) * 100
    
    # Sort by total count (most common combinations first)
    combination_analysis = combination_analysis.loc[combination_totals.sort_values(ascending=False).index]
    combination_proportions = combination_proportions.loc[combination_totals.sort_values(ascending=False).index]
    
    # Create mapping dictionary for cleaner labels
    combo_mapping = {}
    for i, combo in enumerate(combination_analysis.index, 1):
        combo_mapping[combo] = f"Combo {i}"
    
    # Create reverse mapping for reference
    reverse_mapping = {v: k for k, v in combo_mapping.items()}
    
    # Create visualization
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 12))
    fig.suptitle('Demographic Combination Analysis: Saver Type Proportions', fontsize=16, fontweight='bold')
    
    # 1. Top 15 combinations by count
    top_15 = combination_analysis.head(15)
    top_15_proportions = combination_proportions.head(15)
    
    # Create mapped labels for cleaner visualization
    top_15_labels = [combo_mapping[combo] for combo in top_15.index]
    
    # Stacked bar chart for top 15 combinations
    top_15_proportions.index = top_15_labels
    top_15_proportions.plot(kind='bar', stacked=True, ax=ax1, 
                           color=['lightblue', 'darkblue', 'orange'], width=0.8)
    ax1.set_title('Top 15 Demographic Combinations\n(Behavioral Segment Proportions)', fontsize=14)
    ax1.set_xlabel('Demographic Combination', fontsize=12)
    ax1.set_ylabel('Percentage', fontsize=12)
    ax1.legend(title='Behavioral Segment', bbox_to_anchor=(1.05, 1), loc='upper left')
    ax1.tick_params(axis='x', rotation=45, labelsize=10)
    ax1.grid(axis='y', alpha=0.3)
    
    # 2. Heatmap of all combinations (top 20)
    top_20_proportions = combination_proportions.head(20)
    top_20_labels = [combo_mapping[combo] for combo in top_20_proportions.index]
    
    im = ax2.imshow(top_20_proportions.values, cmap='RdYlBu', aspect='auto')
    ax2.set_xticks(range(len(top_20_proportions.columns)))
    ax2.set_xticklabels(top_20_proportions.columns)
    ax2.set_yticks(range(len(top_20_proportions.index)))
    ax2.set_yticklabels(top_20_labels, fontsize=10)
    ax2.set_title('Top 20 Demographic Combinations\n(Heatmap of Behavioral Segment Proportions)', fontsize=14)
    ax2.set_xlabel('Behavioral Segment', fontsize=12)
    ax2.set_ylabel('Demographic Combination', fontsize=12)
    
    # Add colorbar
    cbar = plt.colorbar(im, ax=ax2)
    cbar.set_label('Percentage', fontsize=12)
    
    # Add text annotations on heatmap
    for i in range(len(top_20_proportions.index)):
        for j in range(len(top_20_proportions.columns)):
            text = ax2.text(j, i, f'{top_20_proportions.iloc[i, j]:.1f}%',
                           ha="center", va="center", color="black", fontsize=8)
    
    plt.tight_layout()
    plt.show()
    
    # Print the mapping for reference
    print("\n" + "="*80)
    print("DEMOGRAPHIC COMBINATION MAPPING")
    print("="*80)
    print("Combo ID | Full Demographic Combination")
    print("-" * 80)
    for combo_id, full_combo in reverse_mapping.items():
        print(f"{combo_id:8s} | {full_combo}")
    
    return combination_analysis, combination_proportions, combo_mapping

def generate_summary_report(df):
    """Generate a summary report of key findings"""
    print("\n" + "="*60)
    print("DEMOGRAPHIC ANALYSIS SUMMARY REPORT")
    print("="*60)
    
    # Segment sizes
    segment_sizes = df['BEHAVIORAL_SEGMENT'].value_counts()
    print(f"\nSaver Type Distribution:")
    for segment, count in segment_sizes.items():
        pct = (count / len(df)) * 100
        print(f"  {segment}: {count:,} ({pct:.1f}%)")
    
    # Key demographic insights
    print(f"\nKey Demographic Insights:")
    
    # Gender insights
    gender_pct = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['GENDER'], normalize='index') * 100
    print(f"\nGender Patterns:")
    for segment in df['BEHAVIORAL_SEGMENT'].unique():
        male_pct = gender_pct.loc[segment, 'MALE'] if 'MALE' in gender_pct.columns else 0
        female_pct = gender_pct.loc[segment, 'FEMALE'] if 'FEMALE' in gender_pct.columns else 0
        print(f"  {segment}: {male_pct:.1f}% Male, {female_pct:.1f}% Female")
    
    # Age insights
    age_stats = df.groupby('BEHAVIORAL_SEGMENT')['AGE'].agg(['mean', 'std']).round(1)
    print(f"\nAge Patterns (Mean ± Std):")
    for segment in age_stats.index:
        mean_age = age_stats.loc[segment, 'mean']
        std_age = age_stats.loc[segment, 'std']
        print(f"  {segment}: {mean_age:.1f} ± {std_age:.1f} years")
    
    # Fido Score insights
    fido_stats = df.groupby('BEHAVIORAL_SEGMENT')['FIDO_SCORE_AT_SIGNUP'].agg(['mean', 'std']).round(1)
    print(f"\nFido Score Patterns (Mean ± Std):")
    for segment in fido_stats.index:
        mean_fido = fido_stats.loc[segment, 'mean']
        std_fido = fido_stats.loc[segment, 'std']
        print(f"  {segment}: {mean_fido:.1f} ± {std_fido:.1f}")
    
    # Income insights (only for numeric values)
    income_numeric = df[df['INCOME_VALUE_NUMERIC'].notna()]
    if len(income_numeric) > 0:
        income_stats = income_numeric.groupby('BEHAVIORAL_SEGMENT')['INCOME_VALUE_NUMERIC'].agg(['mean', 'std']).round(1)
        print(f"\nIncome Patterns (Mean ± Std, Converted from ranges):")
        for segment in income_stats.index:
            mean_income = income_stats.loc[segment, 'mean']
            std_income = income_stats.loc[segment, 'std']
            print(f"  {segment}: {mean_income:.1f} ± {std_income:.1f} GHS")
    else:
        print(f"\nIncome Patterns: No numeric income data available")
    
    # Balance insights
    balance_stats = df.groupby('BEHAVIORAL_SEGMENT')['LAST_BALANCE'].agg(['mean', 'std']).round(1)
    print(f"\nBalance Patterns (Mean ± Std):")
    for segment in balance_stats.index:
        mean_balance = balance_stats.loc[segment, 'mean']
        std_balance = balance_stats.loc[segment, 'std']
        print(f"  {segment}: {mean_balance:.1f} ± {std_balance:.1f} GHS")

def generate_persona_insights(df):
    """Generate focused insights for preferred saver personas"""
    
    print("\n" + "="*80)
    print("PERSONA CHARACTERISTICS ANALYSIS")
    print("Focus: What creates different behavioral habits (Savers vs Ultra Savers vs Wallet Users)?")
    print("="*80)
    
    print(f"\nAnalysis Sample: {len(df)} behavioral segments")
    print(f"  - Savers: {len(df[df['BEHAVIORAL_SEGMENT'] == 'savers'])}")
    print(f"  - Ultra Savers: {len(df[df['BEHAVIORAL_SEGMENT'] == 'ultra_savers'])}")
    print(f"  - Wallet Users: {len(df[df['BEHAVIORAL_SEGMENT'] == 'wallet_users'])}")
    
    # 1. DEMOGRAPHIC PROFILE
    print(f"\n{'='*50}")
    print("1. DEMOGRAPHIC PROFILE")
    print(f"{'='*50}")
    
    # Gender Analysis
    gender_pct = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['GENDER'], normalize='index') * 100
    
    print(f"\nGENDER DISTRIBUTION:")
    for segment in ['savers', 'ultra_savers', 'wallet_users']:
        if segment in gender_pct.index:
            male_pct = gender_pct.loc[segment, 'MALE'] if 'MALE' in gender_pct.columns else 0
            female_pct = gender_pct.loc[segment, 'FEMALE'] if 'FEMALE' in gender_pct.columns else 0
            print(f"  {segment.upper()}: {male_pct:.1f}% Male, {female_pct:.1f}% Female")
    
    # Age Analysis
    age_stats = df.groupby('BEHAVIORAL_SEGMENT')['AGE'].agg(['count', 'mean', 'std']).round(1)
    print(f"\nAGE PATTERNS (Mean ± Std):")
    for segment in ['savers', 'ultra_savers', 'wallet_users']:
        if segment in age_stats.index:
            mean_age = age_stats.loc[segment, 'mean']
            std_age = age_stats.loc[segment, 'std']
            print(f"  {segment.upper()}: {mean_age:.1f} ± {std_age:.1f} years")
    
    # 2. FINANCIAL PROFILE
    print(f"\n{'='*50}")
    print("2. FINANCIAL PROFILE")
    print(f"{'='*50}")
    
    # Fido Score Analysis
    fido_stats = df.groupby('BEHAVIORAL_SEGMENT')['FIDO_SCORE_AT_SIGNUP'].agg(['count', 'mean', 'std']).round(1)
    print(f"\nFIDO SCORE PATTERNS (Mean ± Std):")
    for segment in ['savers', 'ultra_savers', 'wallet_users']:
        if segment in fido_stats.index:
            mean_fido = fido_stats.loc[segment, 'mean']
            std_fido = fido_stats.loc[segment, 'std']
            print(f"  {segment.upper()}: {mean_fido:.1f} ± {std_fido:.1f}")
    
    # Income Analysis
    income_numeric = df[df['INCOME_VALUE_NUMERIC'].notna()]
    if len(income_numeric) > 0:
        income_stats = income_numeric.groupby('BEHAVIORAL_SEGMENT')['INCOME_VALUE_NUMERIC'].agg(['count', 'mean', 'std']).round(1)
        print(f"\nINCOME PATTERNS (Mean ± Std, Converted from ranges):")
        for segment in ['savers', 'ultra_savers', 'wallet_users']:
            if segment in income_stats.index:
                mean_income = income_stats.loc[segment, 'mean']
                std_income = income_stats.loc[segment, 'std']
                print(f"  {segment.upper()}: {mean_income:.1f} ± {std_income:.1f} GHS")
    
    # 3. SAVINGS BEHAVIOR
    print(f"\n{'='*50}")
    print("3. SAVINGS BEHAVIOR")
    print(f"{'='*50}")
    
    # Balance Analysis
    balance_stats = df.groupby('BEHAVIORAL_SEGMENT')['LAST_BALANCE'].agg(['count', 'mean', 'std']).round(1)
    print(f"\nBALANCE PATTERNS (Mean ± Std):")
    for segment in ['savers', 'ultra_savers', 'wallet_users']:
        if segment in balance_stats.index:
            mean_balance = balance_stats.loc[segment, 'mean']
            std_balance = balance_stats.loc[segment, 'std']
            print(f"  {segment.upper()}: {mean_balance:.1f} ± {std_balance:.1f} GHS")
    
    # Deposit Frequency Analysis
    deposit_freq_stats = df.groupby('BEHAVIORAL_SEGMENT')['MONTHLY_DEPOSIT_FREQUENCY'].agg(['count', 'mean', 'std']).round(2)
    print(f"\nDEPOSIT FREQUENCY PATTERNS (Mean ± Std):")
    for segment in ['savers', 'ultra_savers', 'wallet_users']:
        if segment in deposit_freq_stats.index:
            mean_freq = deposit_freq_stats.loc[segment, 'mean']
            std_freq = deposit_freq_stats.loc[segment, 'std']
            print(f"  {segment.upper()}: {mean_freq:.2f} ± {std_freq:.2f} deposits/month")
    
    # 4. KEY INSIGHTS
    print(f"\n{'='*50}")
    print("4. KEY INSIGHTS FOR BEHAVIORAL SEGMENTS")
    print(f"{'='*50}")
    
    # Calculate key differences
    saver_data = df[df['BEHAVIORAL_SEGMENT'] == 'savers']
    ultra_saver_data = df[df['BEHAVIORAL_SEGMENT'] == 'ultra_savers']
    wallet_user_data = df[df['BEHAVIORAL_SEGMENT'] == 'wallet_users']
    
    saver_balance = saver_data['LAST_BALANCE'].mean()
    ultra_balance = ultra_saver_data['LAST_BALANCE'].mean()
    wallet_balance = wallet_user_data['LAST_BALANCE'].mean()
    
    ultra_balance_ratio = ultra_balance / saver_balance if saver_balance > 0 else 0
    wallet_balance_ratio = wallet_balance / saver_balance if saver_balance > 0 else 0
    
    saver_fido = saver_data['FIDO_SCORE_AT_SIGNUP'].mean()
    ultra_fido = ultra_saver_data['FIDO_SCORE_AT_SIGNUP'].mean()
    wallet_fido = wallet_user_data['FIDO_SCORE_AT_SIGNUP'].mean()
    
    ultra_fido_diff = ultra_fido - saver_fido
    wallet_fido_diff = wallet_fido - saver_fido
    
    saver_age = saver_data['AGE'].mean()
    ultra_age = ultra_saver_data['AGE'].mean()
    wallet_age = wallet_user_data['AGE'].mean()
    
    ultra_age_diff = ultra_age - saver_age
    wallet_age_diff = wallet_age - saver_age
    
    saver_freq = saver_data['MONTHLY_DEPOSIT_FREQUENCY'].mean()
    wallet_freq = wallet_user_data['MONTHLY_DEPOSIT_FREQUENCY'].mean()
    freq_ratio = wallet_freq / saver_freq if saver_freq > 0 else 0
    
    print(f"\nKEY DIFFERENCES:")
    print(f"  • Ultra Savers have {ultra_balance_ratio:.1f}x higher balances than regular Savers")
    print(f"  • Ultra Savers have {ultra_fido_diff:.1f} point higher Fido scores on average")
    print(f"  • Ultra Savers are {ultra_age_diff:.1f} years older on average")
    print(f"  • Wallet Users have {wallet_balance_ratio:.1f}x different balances than regular Savers")
    print(f"  • Wallet Users have {wallet_fido_diff:.1f} point different Fido scores on average")
    print(f"  • Wallet Users are {wallet_age_diff:.1f} years different on average")
    print(f"  • Wallet Users have {freq_ratio:.1f}x deposit frequency than regular Savers")
    
    # Gender preferences
    saver_male_pct = gender_pct.loc['savers', 'MALE'] if 'savers' in gender_pct.index and 'MALE' in gender_pct.columns else 0
    ultra_male_pct = gender_pct.loc['ultra_savers', 'MALE'] if 'ultra_savers' in gender_pct.index and 'MALE' in gender_pct.columns else 0
    wallet_male_pct = gender_pct.loc['wallet_users', 'MALE'] if 'wallet_users' in gender_pct.index and 'MALE' in gender_pct.columns else 0
    
    print(f"\nGENDER PREFERENCES:")
    print(f"  • Savers: {saver_male_pct:.1f}% Male")
    print(f"  • Ultra Savers: {ultra_male_pct:.1f}% Male")
    print(f"  • Wallet Users: {wallet_male_pct:.1f}% Male")
    
    # Age group preferences
    age_pct = pd.crosstab(df['BEHAVIORAL_SEGMENT'], df['age_group'], normalize='index') * 100
    print(f"\nAGE GROUP PREFERENCES:")
    for segment in ['savers', 'ultra_savers', 'wallet_users']:
        if segment in age_pct.index:
            top_age_group = age_pct.loc[segment].idxmax()
            top_age_pct = age_pct.loc[segment, top_age_group]
            print(f"  • {segment.upper()}: {top_age_pct:.1f}% in {top_age_group} age group")
    
    print(f"\n{'='*50}")
    print("RECOMMENDATIONS FOR TARGETING BEHAVIORAL SEGMENTS:")
    print(f"{'='*50}")
    print(f"1. Target higher Fido score customers (600+) for ultra saver potential")
    print(f"2. Focus on older demographics (35+ years) for better savings habits")
    print(f"3. Prioritize customers with higher income ranges for ultra saver conversion")
    print(f"4. Monitor balance growth patterns to identify saver-to-ultra-saver progression")
    print(f"5. Consider gender-specific messaging based on observed preferences")
    print(f"6. Wallet Users show different patterns - analyze their transaction frequency needs")
    print(f"7. Develop specific strategies for each behavioral segment based on their unique characteristics")

def analyze_demographic_combinations(df, combo_mapping):
    """Analyze specific demographic combinations and their saver type proportions"""
    
    print(f"\n{'='*80}")
    print("DEMOGRAPHIC COMBINATION ANALYSIS")
    print("Gender - Age Group - Income Range - Fido Score → Behavioral Segment Proportions")
    print(f"{'='*80}")
    
    # Create combinations
    df['demographic_combination'] = (
        df['GENDER'].fillna('Unknown') + ' - ' + 
        df['age_group'].astype(str) + ' - ' + 
        df['INCOME_VALUE'].fillna('Unknown') + ' - ' + 
        df['fido_score_group'].astype(str) + ' - ' +
        df['EMPLOYMENT'].fillna('Unknown') + ' - ' +
        df['MARITAL_STATUS'].fillna('Unknown') + ' - ' +
        df['EDUCATION_LEVEL'].fillna('Unknown')
    )
    
    # Get combination analysis
    combination_analysis = df.groupby(['demographic_combination', 'BEHAVIORAL_SEGMENT']).size().unstack(fill_value=0)
    combination_totals = combination_analysis.sum(axis=1)
    combination_proportions = combination_analysis.div(combination_totals, axis=0) * 100
    
    # Sort by total count
    combination_analysis = combination_analysis.loc[combination_totals.sort_values(ascending=False).index]
    combination_proportions = combination_proportions.loc[combination_totals.sort_values(ascending=False).index]
    
    print(f"\nTotal unique demographic combinations: {len(combination_analysis)}")
    print(f"Combinations with 10+ customers: {len(combination_analysis[combination_totals >= 10])}")
    
    # Top combinations by count
    print(f"\n{'='*60}")
    print("TOP 20 DEMOGRAPHIC COMBINATIONS BY CUSTOMER COUNT")
    print(f"{'='*60}")
    
    top_20 = combination_analysis.head(20)
    for i, (combo, row) in enumerate(top_20.iterrows(), 1):
        total = row.sum()
        saver_pct = (row.get('savers', 0) / total) * 100
        ultra_pct = (row.get('ultra_savers', 0) / total) * 100
        wallet_pct = (row.get('wallet_users', 0) / total) * 100
        combo_id = combo_mapping.get(combo, f"Combo {i}")
        print(f"{combo_id:8s} | Total: {total:3d} customers | Savers: {saver_pct:5.1f}% | Ultra Savers: {ultra_pct:5.1f}% | Wallet Users: {wallet_pct:5.1f}%")
        print(f"         | {combo}")
        print()
    
    # Ultra saver dominant combinations
    print(f"\n{'='*60}")
    print("COMBINATIONS WITH HIGHEST ULTRA SAVER PROPORTION (10+ customers)")
    print(f"{'='*60}")
    
    ultra_dominant = combination_proportions[combination_totals >= 10].sort_values('ultra_savers', ascending=False).head(15)
    for i, (combo, row) in enumerate(ultra_dominant.iterrows(), 1):
        total = combination_analysis.loc[combo].sum()
        ultra_pct = row.get('ultra_savers', 0)
        saver_pct = row.get('savers', 0)
        wallet_pct = row.get('wallet_users', 0)
        combo_id = combo_mapping.get(combo, f"Combo {i}")
        print(f"{combo_id:8s} | Total: {total:3d} customers | Ultra Savers: {ultra_pct:5.1f}% | Savers: {saver_pct:5.1f}% | Wallet Users: {wallet_pct:5.1f}%")
        print(f"         | {combo}")
        print()
    
    # Saver dominant combinations
    print(f"\n{'='*60}")
    print("COMBINATIONS WITH HIGHEST SAVER PROPORTION (10+ customers)")
    print(f"{'='*60}")
    
    saver_dominant = combination_proportions[combination_totals >= 10].sort_values('savers', ascending=False).head(15)
    for i, (combo, row) in enumerate(saver_dominant.iterrows(), 1):
        total = combination_analysis.loc[combo].sum()
        saver_pct = row.get('savers', 0)
        ultra_pct = row.get('ultra_savers', 0)
        wallet_pct = row.get('wallet_users', 0)
        combo_id = combo_mapping.get(combo, f"Combo {i}")
        print(f"{combo_id:8s} | Total: {total:3d} customers | Savers: {saver_pct:5.1f}% | Ultra Savers: {ultra_pct:5.1f}% | Wallet Users: {wallet_pct:5.1f}%")
        print(f"         | {combo}")
        print()
    
    # Key insights
    print(f"\n{'='*60}")
    print("KEY INSIGHTS FROM COMBINATION ANALYSIS")
    print(f"{'='*60}")
    
    # Most common combinations
    most_common = combination_analysis.head(5)
    print(f"\nMOST COMMON DEMOGRAPHIC COMBINATIONS:")
    for i, (combo, row) in enumerate(most_common.iterrows(), 1):
        total = row.sum()
        ultra_pct = (row.get('ultra_savers', 0) / total) * 100
        wallet_pct = (row.get('wallet_users', 0) / total) * 100
        combo_id = combo_mapping.get(combo, f"Combo {i}")
        print(f"  {combo_id}: {total} customers, {ultra_pct:.1f}% ultra savers, {wallet_pct:.1f}% wallet users")
        print(f"    {combo}")
    
    # Ultra saver patterns
    high_ultra = combination_proportions[combination_totals >= 5]['ultra_savers'].nlargest(10)
    print(f"\nCOMBINATIONS WITH HIGHEST ULTRA SAVER RATES (5+ customers):")
    for combo, ultra_pct in high_ultra.items():
        total = combination_analysis.loc[combo].sum()
        wallet_pct = (combination_analysis.loc[combo].get('wallet_users', 0) / total) * 100
        combo_id = combo_mapping.get(combo, "Unknown")
        print(f"  • {combo_id}: {ultra_pct:.1f}% ultra savers, {wallet_pct:.1f}% wallet users ({total} customers)")
        print(f"    {combo}")
    
    # Gender patterns
    print(f"\nGENDER PATTERNS IN TOP COMBINATIONS:")
    gender_analysis = df.groupby(['GENDER', 'BEHAVIORAL_SEGMENT']).size().unstack(fill_value=0)
    gender_pct = gender_analysis.div(gender_analysis.sum(axis=1), axis=0) * 100
    for gender in ['MALE', 'FEMALE']:
        if gender in gender_pct.index:
            ultra_pct = gender_pct.loc[gender, 'ultra_savers']
            saver_pct = gender_pct.loc[gender, 'savers']
            wallet_pct = gender_pct.loc[gender, 'wallet_users']
            print(f"  • {gender}: {saver_pct:.1f}% savers, {ultra_pct:.1f}% ultra savers, {wallet_pct:.1f}% wallet users")
    
    # Age patterns
    print(f"\nAGE GROUP PATTERNS IN TOP COMBINATIONS:")
    age_analysis = df.groupby(['age_group', 'BEHAVIORAL_SEGMENT']).size().unstack(fill_value=0)
    age_pct = age_analysis.div(age_analysis.sum(axis=1), axis=0) * 100
    for age_group in age_pct.index:
        if pd.notna(age_group):
            ultra_pct = age_pct.loc[age_group, 'ultra_savers']
            saver_pct = age_pct.loc[age_group, 'savers']
            wallet_pct = age_pct.loc[age_group, 'wallet_users']
            print(f"  • {age_group}: {saver_pct:.1f}% savers, {ultra_pct:.1f}% ultra savers, {wallet_pct:.1f}% wallet users")
    
    return combination_analysis, combination_proportions

def main():
    """Main function to run persona characteristics analysis"""
    print("Starting Persona Characteristics Analysis...")
    print("Focus: What creates different behavioral habits (Savers vs Ultra Savers vs Wallet Users)?")
    
    # Load and prepare data
    df = load_data()
    df = prepare_demographic_data(df)
    
    # Run individual demographic analyses
    analyze_gender_distribution(df)
    analyze_age_distribution(df)
    analyze_fido_score_distribution(df)
    analyze_income_distribution(df)
    analyze_region_distribution(df)
    analyze_employment_distribution(df)
    analyze_marital_status_distribution(df)
    analyze_education_distribution(df)
    
    # Create combination analysis charts
    combination_analysis, combination_proportions, combo_mapping = create_combination_analysis(df)
    
    # Generate detailed combination insights
    analyze_demographic_combinations(df, combo_mapping)
    
    # Generate focused insights
    generate_persona_insights(df)
    
    print("\n" + "="*80)
    print("ANALYSIS COMPLETE!")
    print("="*80)

if __name__ == "__main__":
    main()
