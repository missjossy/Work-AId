#!/usr/bin/env python3
"""
Simple t-test analysis to assess if high engagement is associated with retention
"""

import pandas as pd
import numpy as np
from scipy import stats

# Load data
df = pd.read_csv('python_code/Savings Product - Login Behaviour.csv')
print("Original Data:")
print(df)
print("\n" + "="*50 + "\n")

# Convert aggregated data to individual observations
observations = []
for _, row in df.iterrows():
    retention_status = row['RETENTION_STATUS']
    engagement_segment = row['ENGAGEMENT_SEGMENT']
    client_count = int(row['SUM(CLIENT_COUNT)'])
    
    for _ in range(client_count):
        observations.append({
            'retention_status': retention_status,
            'engagement_segment': engagement_segment,
            'is_retained': 1 if retention_status == 'retained' else 0
        })

df_individual = pd.DataFrame(observations)
print(f"Total individual observations: {len(df_individual)}")

# Calculate retention rates by engagement segment
print("\nRetention Rates by Engagement Segment:")
print("-" * 40)
retention_rates = df_individual.groupby('engagement_segment')['is_retained'].agg(['count', 'sum', 'mean']).round(4)
retention_rates.columns = ['Total_Clients', 'Retained_Clients', 'Retention_Rate']
print(retention_rates)

# Prepare data for t-tests
high_engagement = df_individual[df_individual['engagement_segment'] == 'high_engagement']['is_retained']
medium_engagement = df_individual[df_individual['engagement_segment'] == 'medium_engagement']['is_retained']
low_engagement = df_individual[df_individual['engagement_segment'] == 'low_engagement']['is_retained']

print(f"\nSample sizes:")
print(f"High engagement: {len(high_engagement)}")
print(f"Medium engagement: {len(medium_engagement)}")
print(f"Low engagement: {len(low_engagement)}")

# T-test 1: High vs Low engagement
print("\n" + "="*50)
print("T-TEST: HIGH ENGAGEMENT vs LOW ENGAGEMENT")
print("-" * 40)
t_stat_high_low, p_value_high_low = stats.ttest_ind(high_engagement, low_engagement)
print(f"T-statistic: {t_stat_high_low:.4f}")
print(f"P-value: {p_value_high_low:.6f}")
print(f"Significant at α=0.05: {'Yes' if p_value_high_low < 0.05 else 'No'}")
print(f"Mean retention rate - High engagement: {high_engagement.mean():.4f}")
print(f"Mean retention rate - Low engagement: {low_engagement.mean():.4f}")
print(f"Difference: {high_engagement.mean() - low_engagement.mean():.4f}")

# T-test 2: High vs Medium engagement
print("\nT-TEST: HIGH ENGAGEMENT vs MEDIUM ENGAGEMENT")
print("-" * 40)
t_stat_high_medium, p_value_high_medium = stats.ttest_ind(high_engagement, medium_engagement)
print(f"T-statistic: {t_stat_high_medium:.4f}")
print(f"P-value: {p_value_high_medium:.6f}")
print(f"Significant at α=0.05: {'Yes' if p_value_high_medium < 0.05 else 'No'}")
print(f"Mean retention rate - High engagement: {high_engagement.mean():.4f}")
print(f"Mean retention rate - Medium engagement: {medium_engagement.mean():.4f}")
print(f"Difference: {high_engagement.mean() - medium_engagement.mean():.4f}")

# T-test 3: Medium vs Low engagement
print("\nT-TEST: MEDIUM ENGAGEMENT vs LOW ENGAGEMENT")
print("-" * 40)
t_stat_medium_low, p_value_medium_low = stats.ttest_ind(medium_engagement, low_engagement)
print(f"T-statistic: {t_stat_medium_low:.4f}")
print(f"P-value: {p_value_medium_low:.6f}")
print(f"Significant at α=0.05: {'Yes' if p_value_medium_low < 0.05 else 'No'}")
print(f"Mean retention rate - Medium engagement: {medium_engagement.mean():.4f}")
print(f"Mean retention rate - Low engagement: {low_engagement.mean():.4f}")
print(f"Difference: {medium_engagement.mean() - low_engagement.mean():.4f}")

# ANOVA test
print("\n" + "="*50)
print("ANOVA TEST (All Engagement Levels)")
print("-" * 40)
f_stat, p_value_anova = stats.f_oneway(high_engagement, medium_engagement, low_engagement)
print(f"F-statistic: {f_stat:.4f}")
print(f"P-value: {p_value_anova:.6f}")
print(f"Significant at α=0.05: {'Yes' if p_value_anova < 0.05 else 'No'}")

# Effect size (Cohen's d) for High vs Low
print("\n" + "="*50)
print("EFFECT SIZE (Cohen's d): High vs Low Engagement")
print("-" * 40)
n1, n2 = len(high_engagement), len(low_engagement)
s1, s2 = high_engagement.std(ddof=1), low_engagement.std(ddof=1)
pooled_std = np.sqrt(((n1-1)*s1**2 + (n2-1)*s2**2) / (n1+n2-2))
cohens_d = (high_engagement.mean() - low_engagement.mean()) / pooled_std
print(f"Cohen's d: {cohens_d:.4f}")
print("Effect Size Interpretation: Small (0.2), Medium (0.5), Large (0.8)")

# Summary
print("\n" + "="*50)
print("SUMMARY")
print("="*50)
print("Key Findings:")
print(f"• High engagement retention rate: {high_engagement.mean():.3f}")
print(f"• Medium engagement retention rate: {medium_engagement.mean():.3f}")
print(f"• Low engagement retention rate: {low_engagement.mean():.3f}")

difference = high_engagement.mean() - low_engagement.mean()
print(f"\n• High engagement clients are {difference:.1%} more likely to be retained")
print(f"• This represents a {difference/high_engagement.mean():.1%} relative improvement in retention")
print(f"• The relationship is statistically significant: {p_value_high_low < 0.05}")

if p_value_high_low < 0.05:
    print("\n• RECOMMENDATION: Focus on increasing client engagement to improve retention")
else:
    print("\n• RECOMMENDATION: Engagement may not be the primary driver of retention")