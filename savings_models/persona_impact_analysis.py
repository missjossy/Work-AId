#!/usr/bin/env python3
"""
Persona Impact Analysis
Determines which demographic characteristics have the most impact on saver type classification
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.linear_model import LogisticRegression
from scipy.stats import chi2_contingency, f_oneway
import warnings
warnings.filterwarnings('ignore')

def load_and_prepare_data():
    """Load and prepare data for analysis"""
    print("Loading and preparing data...")
    
    # Load data
    df = pd.read_csv('/Users/fido_josephine/Documents/Work-AId/savings_models/main_segemnts_data.csv')
    
    # Clean and filter
    df['BEHAVIORAL_SEGMENT'] = df['BEHAVIORAL_SEGMENT'].str.lower()
    df = df[df['BEHAVIORAL_SEGMENT'].isin(['savers', 'ultra_savers', 'wallet_users'])]
    
    print(f"Loaded {len(df)} records")
    print(f"Behavioral segments: {df['BEHAVIORAL_SEGMENT'].value_counts().to_dict()}")
    
    return df

def prepare_features(df):
    """Prepare features for analysis"""
    print("\nPreparing features...")
    
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
    
    # Convert income ranges to numeric
    def income_to_numeric(income_str):
        if pd.isna(income_str):
            return np.nan
        income_str = str(income_str).upper()
        if 'ABOVE 1800' in income_str:
            return 2000  # Midpoint estimate
        elif '1401' in income_str and '1800' in income_str:
            return 1600
        elif '1001' in income_str and '1400' in income_str:
            return 1200
        elif '701' in income_str and '1000' in income_str:
            return 850
        elif '351' in income_str and '700' in income_str:
            return 525
        elif 'BELOW 350' in income_str:
            return 200
        else:
            return np.nan
    
    df['INCOME_VALUE_NUMERIC'] = df['INCOME_VALUE'].apply(income_to_numeric)
    
    # Create income groups
    df['income_group'] = 'Unknown'
    income_numeric = df[df['INCOME_VALUE_NUMERIC'].notna()]
    if len(income_numeric) > 0:
        df.loc[income_numeric.index, 'income_group'] = pd.cut(
            income_numeric['INCOME_VALUE_NUMERIC'], 
            bins=[0, 500, 1000, 1500, 2000, 10000], 
            labels=['Low (0-500)', 'Medium (500-1000)', 'High (1000-1500)', 'Very High (1500+)', 'Premium (2000+)'],
            include_lowest=True
        ).astype(str)
    
    print("Feature preparation complete")
    return df

def calculate_feature_importance(df):
    """Calculate feature importance using Random Forest"""
    print("\nCalculating feature importance using Random Forest...")
    
    # Prepare features - focus on key demographic characteristics
    feature_columns = [
        'GENDER', 'age_group', 'INCOME_VALUE', 'REGION'
    ]
    
    # Create feature matrix
    X = df[feature_columns].copy()
    y = df['BEHAVIORAL_SEGMENT']
    
    # Handle missing values - all features are categorical
    for col in X.columns:
        X[col] = X[col].astype(str).fillna('Unknown')
    
    # Encode categorical variables
    le_dict = {}
    for col in X.columns:
        le = LabelEncoder()
        X[col] = le.fit_transform(X[col])
        le_dict[col] = le
    
    # Train Random Forest
    rf = RandomForestClassifier(n_estimators=100, random_state=42, max_depth=10)
    rf.fit(X, y)
    
    # Get feature importance
    feature_importance = pd.DataFrame({
        'feature': feature_columns,
        'importance': rf.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print("Random Forest Feature Importance:")
    print(feature_importance)
    
    return feature_importance, rf, le_dict

def calculate_statistical_significance(df):
    """Calculate statistical significance of each feature"""
    print("\nCalculating statistical significance...")
    
    results = []
    
    # Categorical features - Chi-square test
    categorical_features = ['GENDER', 'age_group', 'INCOME_VALUE', 'REGION']
    
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
    
    # No numerical features in this simplified analysis
    
    # Convert to DataFrame
    significance_df = pd.DataFrame(results)
    significance_df = significance_df.sort_values('p_value')
    
    print("Statistical Significance Results:")
    print(significance_df)
    
    return significance_df


def create_visualizations(feature_importance, significance_df):
    """Create visualizations for the analysis"""
    print("\nCreating visualizations...")
    
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle('Persona Impact Analysis: Feature Importance and Significance', fontsize=16, fontweight='bold')
    
    # 1. Feature Importance (Random Forest)
    ax1 = axes[0]
    bars1 = ax1.barh(range(len(feature_importance)), feature_importance['importance'], color='skyblue')
    ax1.set_yticks(range(len(feature_importance)))
    ax1.set_yticklabels(feature_importance['feature'])
    ax1.set_xlabel('Feature Importance')
    ax1.set_title('Feature Importance\n(Random Forest)')
    ax1.grid(axis='x', alpha=0.3)
    
    # Add value labels on bars
    for i, bar in enumerate(bars1):
        width = bar.get_width()
        ax1.text(width + 0.001, bar.get_y() + bar.get_height()/2, 
                f'{width:.3f}', ha='left', va='center', fontsize=9)
    
    # 2. Statistical Significance
    ax2 = axes[1]
    significant_features = significance_df[significance_df['significant'] == True]
    if len(significant_features) > 0:
        bars2 = ax2.barh(range(len(significant_features)), -np.log10(significant_features['p_value']), 
                        color='lightcoral')
        ax2.set_yticks(range(len(significant_features)))
        ax2.set_yticklabels(significant_features['feature'])
        ax2.set_xlabel('-log10(p-value)')
        ax2.set_title('Statistical Significance\n(Features with p < 0.05)')
        ax2.axvline(x=-np.log10(0.05), color='red', linestyle='--', alpha=0.7, label='p=0.05')
        ax2.legend()
        ax2.grid(axis='x', alpha=0.3)
    else:
        ax2.text(0.5, 0.5, 'No significant features\n(p < 0.05)', 
                ha='center', va='center', transform=ax2.transAxes, fontsize=12)
        ax2.set_title('Statistical Significance')
    
    plt.tight_layout()
    plt.show()

def generate_insights(feature_importance, significance_df):
    """Generate insights from the analysis"""
    print("\n" + "="*80)
    print("PERSONA IMPACT ANALYSIS INSIGHTS")
    print("="*80)
    
    # Top important features
    print(f"\nFEATURE IMPORTANCE RANKING (Random Forest):")
    for i, (_, row) in enumerate(feature_importance.iterrows(), 1):
        print(f"  {i}. {row['feature']}: {row['importance']:.3f}")
    
    # Most significant features
    significant_features = significance_df[significance_df['significant'] == True]
    print(f"\nSTATISTICALLY SIGNIFICANT FEATURES (p < 0.05):")
    if len(significant_features) > 0:
        for i, (_, row) in enumerate(significant_features.iterrows(), 1):
            print(f"  {i}. {row['feature']} ({row['test_type']}): p = {row['p_value']:.4f}")
    else:
        print("  No features are statistically significant at p < 0.05")
    
    # Key insights
    print(f"\nKEY INSIGHTS:")
    print(f"  • Most predictive feature: {feature_importance.iloc[0]['feature']} ({feature_importance.iloc[0]['importance']:.3f})")
    print(f"  • Least predictive feature: {feature_importance.iloc[-1]['feature']} ({feature_importance.iloc[-1]['importance']:.3f})")
    print(f"  • Number of significant features: {len(significant_features)}")
    
    if len(significant_features) > 0:
        most_significant = significant_features.iloc[0]
        print(f"  • Most significant feature: {most_significant['feature']} (p = {most_significant['p_value']:.4f})")
    
    # Feature impact interpretation
    print(f"\nFEATURE IMPACT INTERPRETATION:")
    for i, (_, row) in enumerate(feature_importance.iterrows(), 1):
        importance = row['importance']
        if importance > 0.3:
            impact_level = "HIGH"
        elif importance > 0.2:
            impact_level = "MEDIUM"
        else:
            impact_level = "LOW"
        print(f"  • {row['feature']}: {impact_level} impact ({importance:.3f})")
    
    # Recommendations
    print(f"\nRECOMMENDATIONS:")
    print(f"  1. Focus on {feature_importance.iloc[0]['feature']} for primary persona targeting")
    print(f"  2. Use {feature_importance.iloc[1]['feature']} as secondary targeting criteria")
    print(f"  3. Consider {feature_importance.iloc[-1]['feature']} for fine-tuning only")
    if len(significant_features) > 0:
        print(f"  4. Prioritize statistically significant features for reliable segmentation")
    else:
        print(f"  4. All features show similar statistical significance - use Random Forest importance for ranking")
    print(f"  5. Test different combinations of top features for optimal persona creation")

def main():
    """Main function to run persona impact analysis"""
    print("Starting Persona Impact Analysis...")
    print("Determining which characteristics have the most impact on saver type")
    
    # Load and prepare data
    df = load_and_prepare_data()
    df = prepare_features(df)
    
    # Calculate feature importance
    feature_importance, rf_model, le_dict = calculate_feature_importance(df)
    
    # Calculate statistical significance
    significance_df = calculate_statistical_significance(df)
    
    # Create visualizations
    create_visualizations(feature_importance, significance_df)
    
    # Generate insights
    generate_insights(feature_importance, significance_df)
    
    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)

if __name__ == "__main__":
    main()
