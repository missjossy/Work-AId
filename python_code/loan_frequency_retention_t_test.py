#!/usr/bin/env python3
"""
Simple t-test analysis to assess if loan frequency is associated with retention
"""

import pandas as pd
import numpy as np
from scipy import stats

# Load data
df = pd.read_csv('python_code/Savings Product - Loan Behaviour.csv')
print("Original Data:")
print(df)
print("\n" + "="*50 + "\n")

# Convert aggregated data to individual observations
observations = []
for _, row in df.iterrows():
    retention_status = row['RETENTION_STATUS']
    loan_frequency_segment = row['LOAN_FREQUENCY_SEGMENT']
    client_count = int(row['SUM(CLIENT_COUNT)'])
    
    for _ in range(client_count):
        observations.append({
            'retention_status': retention_status,
            'loan_frequency_segment': loan_frequency_segment,
            'is_retained': 1 if retention_status == 'retained' else 0
        })

df_individual = pd.DataFrame(observations)
print(f"Total individual observations: {len(df_individual)}")

# Calculate retention rates by loan frequency segment
print("\nRetention Rates by Loan Frequency Segment:")
print("-" * 50)
retention_rates = df_individual.groupby('loan_frequency_segment')['is_retained'].agg(['count', 'sum', 'mean']).round(4)
retention_rates.columns = ['Total_Clients', 'Retained_Clients', 'Retention_Rate']
print(retention_rates)

# Prepare data for t-tests
high_frequency = df_individual[df_individual['loan_frequency_segment'] == 'high_frequency']['is_retained']
medium_frequency = df_individual[df_individual['loan_frequency_segment'] == 'medium_frequency']['is_retained']
low_frequency = df_individual[df_individual['loan_frequency_segment'] == 'low_frequency']['is_retained']
single_loan = df_individual[df_individual['loan_frequency_segment'] == 'single_loan']['is_retained']
no_loans = df_individual[df_individual['loan_frequency_segment'] == 'no_loans']['is_retained']

print(f"\nSample sizes:")
print(f"High frequency: {len(high_frequency)}")
print(f"Medium frequency: {len(medium_frequency)}")
print(f"Low frequency: {len(low_frequency)}")
print(f"Single loan: {len(single_loan)}")
print(f"No loans: {len(no_loans)}")

# T-test 1: High frequency vs No loans
print("\n" + "="*50)
print("T-TEST: HIGH FREQUENCY vs NO LOANS")
print("-" * 40)
t_stat_high_none, p_value_high_none = stats.ttest_ind(high_frequency, no_loans)
print(f"T-statistic: {t_stat_high_none:.4f}")
print(f"P-value: {p_value_high_none:.6f}")
print(f"Significant at α=0.05: {'Yes' if p_value_high_none < 0.05 else 'No'}")
print(f"Mean retention rate - High frequency: {high_frequency.mean():.4f}")
print(f"Mean retention rate - No loans: {no_loans.mean():.4f}")
print(f"Difference: {high_frequency.mean() - no_loans.mean():.4f}")

# T-test 2: High frequency vs Single loan
print("\nT-TEST: HIGH FREQUENCY vs SINGLE LOAN")
print("-" * 40)
t_stat_high_single, p_value_high_single = stats.ttest_ind(high_frequency, single_loan)
print(f"T-statistic: {t_stat_high_single:.4f}")
print(f"P-value: {p_value_high_single:.6f}")
print(f"Significant at α=0.05: {'Yes' if p_value_high_single < 0.05 else 'No'}")
print(f"Mean retention rate - High frequency: {high_frequency.mean():.4f}")
print(f"Mean retention rate - Single loan: {single_loan.mean():.4f}")
print(f"Difference: {high_frequency.mean() - single_loan.mean():.4f}")

# T-test 3: High frequency vs Low frequency
print("\nT-TEST: HIGH FREQUENCY vs LOW FREQUENCY")
print("-" * 40)
t_stat_high_low, p_value_high_low = stats.ttest_ind(high_frequency, low_frequency)
print(f"T-statistic: {t_stat_high_low:.4f}")
print(f"P-value: {p_value_high_low:.6f}")
print(f"Significant at α=0.05: {'Yes' if p_value_high_low < 0.05 else 'No'}")
print(f"Mean retention rate - High frequency: {high_frequency.mean():.4f}")
print(f"Mean retention rate - Low frequency: {low_frequency.mean():.4f}")
print(f"Difference: {high_frequency.mean() - low_frequency.mean():.4f}")

# T-test 4: No loans vs Single loan
print("\nT-TEST: NO LOANS vs SINGLE LOAN")
print("-" * 40)
t_stat_none_single, p_value_none_single = stats.ttest_ind(no_loans, single_loan)
print(f"T-statistic: {t_stat_none_single:.4f}")
print(f"P-value: {p_value_none_single:.6f}")
print(f"Significant at α=0.05: {'Yes' if p_value_none_single < 0.05 else 'No'}")
print(f"Mean retention rate - No loans: {no_loans.mean():.4f}")
print(f"Mean retention rate - Single loan: {single_loan.mean():.4f}")
print(f"Difference: {no_loans.mean() - single_loan.mean():.4f}")

# ANOVA test
print("\n" + "="*50)
print("ANOVA TEST (All Loan Frequency Groups)")
print("-" * 40)
f_stat, p_value_anova = stats.f_oneway(high_frequency, medium_frequency, low_frequency, single_loan, no_loans)
print(f"F-statistic: {f_stat:.4f}")
print(f"P-value: {p_value_anova:.6f}")
print(f"Significant at α=0.05: {'Yes' if p_value_anova < 0.05 else 'No'}")

# Effect size (Cohen's d) for High frequency vs No loans
print("\n" + "="*50)
print("EFFECT SIZE (Cohen's d): High Frequency vs No Loans")
print("-" * 40)
n1, n2 = len(high_frequency), len(no_loans)
s1, s2 = high_frequency.std(ddof=1), no_loans.std(ddof=1)
pooled_std = np.sqrt(((n1-1)*s1**2 + (n2-1)*s2**2) / (n1+n2-2))
cohens_d = (high_frequency.mean() - no_loans.mean()) / pooled_std
print(f"Cohen's d: {cohens_d:.4f}")
print("Effect Size Interpretation: Small (0.2), Medium (0.5), Large (0.8)")

# Summary
print("\n" + "="*50)
print("SUMMARY")
print("="*50)
print("Key Findings:")
print(f"• High frequency retention rate: {high_frequency.mean():.3f}")
print(f"• Medium frequency retention rate: {medium_frequency.mean():.3f}")
print(f"• Low frequency retention rate: {low_frequency.mean():.3f}")
print(f"• Single loan retention rate: {single_loan.mean():.3f}")
print(f"• No loans retention rate: {no_loans.mean():.3f}")

# Find the highest and lowest retention rates
retention_by_segment = df_individual.groupby('loan_frequency_segment')['is_retained'].mean()
highest_segment = retention_by_segment.idxmax()
lowest_segment = retention_by_segment.idxmin()
highest_rate = retention_by_segment.max()
lowest_rate = retention_by_segment.min()

print(f"\n• Highest retention: {highest_segment} ({highest_rate:.3f})")
print(f"• Lowest retention: {lowest_segment} ({lowest_rate:.3f})")
print(f"• Difference: {highest_rate - lowest_rate:.1%}")

if p_value_anova < 0.05:
    print("\n• RECOMMENDATION: Loan frequency significantly affects retention - consider targeted strategies")
else:
    print("\n• RECOMMENDATION: Loan frequency may not be a primary driver of retention")
