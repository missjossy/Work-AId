"""
Feature Importance Analysis with New Demographic Attributes
Shows feature importance ranking using Random Forest
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
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

def calculate_feature_importance(df):
    """Calculate feature importance using Random Forest"""
    print("\nCalculating feature importance using Random Forest...")
    
    # Prepare features - focus on key demographic characteristics
    feature_columns = ['age_group', 'INCOME_VALUE', 'REGION', 'EMPLOYMENT', 'MARITAL_STATUS', 'EDUCATION_LEVEL']
    
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

def create_feature_importance_visualization(feature_importance):
    """Create feature importance visualization"""
    print("\nCreating feature importance visualization...")
    
    plt.figure(figsize=(12, 8))
    
    # Create horizontal bar chart
    bars = plt.barh(range(len(feature_importance)), feature_importance['importance'], 
                    color='skyblue', edgecolor='navy', alpha=0.7)
    
    plt.yticks(range(len(feature_importance)), feature_importance['feature'])
    plt.xlabel('Feature Importance', fontsize=12, fontweight='bold')
    plt.title('Feature Importance Analysis\n(Random Forest Classification)', 
              fontsize=16, fontweight='bold', pad=20)
    plt.grid(axis='x', alpha=0.3)
    
    # Add value labels on bars
    for i, bar in enumerate(bars):
        width = bar.get_width()
        plt.text(width + 0.001, bar.get_y() + bar.get_height()/2, 
                f'{width:.3f}', ha='left', va='center', fontsize=10, fontweight='bold')
    
    # Add ranking numbers
    for i, (_, row) in enumerate(feature_importance.iterrows(), 1):
        plt.text(-0.05, i-1, f'#{i}', ha='right', va='center', 
                fontsize=10, fontweight='bold', color='darkred')
    
    plt.tight_layout()
    plt.savefig('/Users/fido_josephine/Documents/Work-AId/savings_models/feature_importance_analysis.png', 
                dpi=300, bbox_inches='tight')
    plt.show()
    
    return feature_importance

def generate_insights(feature_importance):
    """Generate insights from feature importance analysis"""
    print("\n" + "="*80)
    print("FEATURE IMPORTANCE ANALYSIS INSIGHTS")
    print("="*80)
    
    # Feature importance ranking
    print(f"\nFEATURE IMPORTANCE RANKING:")
    for i, (_, row) in enumerate(feature_importance.iterrows(), 1):
        print(f"  {i}. {row['feature']}: {row['importance']:.3f}")
    
    # Key insights
    print(f"\nKEY INSIGHTS:")
    print(f"  • Most predictive feature: {feature_importance.iloc[0]['feature']} ({feature_importance.iloc[0]['importance']:.3f})")
    print(f"  • Least predictive feature: {feature_importance.iloc[-1]['feature']} ({feature_importance.iloc[-1]['importance']:.3f})")
    
    # Feature impact interpretation
    print(f"\nFEATURE IMPACT INTERPRETATION:")
    for i, (_, row) in enumerate(feature_importance.iterrows(), 1):
        importance = row['importance']
        if importance > 0.4:
            impact_level = "VERY HIGH"
        elif importance > 0.3:
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
    print(f"  4. Test different combinations of top features for optimal persona creation")

def main():
    """Main function to run feature importance analysis"""
    print("Starting Feature Importance Analysis...")
    print("Analyzing: Age Group, Income Range, Region, Employment, Marital Status, Education Level")
    
    # Load and prepare data
    df = load_data()
    df = prepare_demographic_data(df)
    
    # Calculate feature importance
    feature_importance, rf_model, le_dict = calculate_feature_importance(df)
    
    # Create visualization
    create_feature_importance_visualization(feature_importance)
    
    # Generate insights
    generate_insights(feature_importance)
    
    print("\n" + "="*80)
    print("FEATURE IMPORTANCE ANALYSIS COMPLETE")
    print("="*80)

if __name__ == "__main__":
    main()

