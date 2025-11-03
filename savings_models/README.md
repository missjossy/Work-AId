# Savings Persona Analysis Models

This folder contains two analytical approaches for understanding savings personas using the existing behavioral segments (ultra_savers, savers, wallet_users, testers).

## Files

### 1. RFM Analysis (`rfm_analysis.py`)
**Purpose**: Analyze savings behavior using Recency, Frequency, and Monetary metrics

**Key Features**:
- **Recency**: Days since last deposit
- **Frequency**: Number of deposits per month (average)
- **Monetary**: Average deposit amount and current balance
- Maps existing personas to RFM segments (Champions, Loyal Customers, At Risk, etc.)

**RFM Segments**:
- **Champions**: High recency, frequency, and monetary value
- **Loyal Customers**: Regular savers with good monetary value
- **Potential Loyalists**: Recent savers with good frequency
- **At Risk**: Low recency but good historical behavior
- **Cannot Lose Them**: Low across all metrics

### 2. Correlation Analysis (`correlation_analysis.py`)
**Purpose**: Analyze relationships between demographics and savings behavior

**Key Features**:
- **Demographic Analysis**: Gender, age, Fido score, income, region distributions by persona
- **Correlation Matrix**: Relationships between demographics and savings metrics
- **Statistical Tests**: Chi-square tests for categorical variables, Pearson/Spearman correlations
- **Savings Quality Score**: Composite metric combining balance, consistency, and frequency

**Savings Metrics**:
- **Savings Consistency**: Deposits per active day
- **Withdrawal Ratio**: Withdrawals relative to deposits
- **Balance Growth Rate**: Balance growth per day
- **Deposit/Withdrawal Frequency**: Monthly transaction rates

## Usage

### Prerequisites
```bash
pip install -r requirements.txt
```

### Running the Analysis

1. **Load your data** from the `main_segments.sql` query
2. **Update the data loading functions** in both scripts to connect to your data source
3. **Run the analysis**:

```python
# RFM Analysis
python rfm_analysis.py

# Correlation Analysis
python correlation_analysis.py
```

### Expected Data Columns

**From main_segments.sql**:
- `client_id`: Unique client identifier
- `behavioral_segment`: ultra_savers, savers, wallet_users, testers
- `age`: Client age
- `gender`: Client gender
- `fido_score_at_signup`: Fido score at savings signup
- `income_value`: Client income (if available)
- `region`: Client region/location
- `total_deposits_count`: Number of deposits
- `total_deposits_amount`: Total deposit amount
- `total_withdrawals_count`: Number of withdrawals
- `last_balance`: Current account balance
- `last_transaction_date`: Date of last transaction
- `monthly_deposit_frequency`: Deposits per month
- `monthly_withdrawal_frequency`: Withdrawals per month
- `days_active`: Days since first account creation

## Key Insights to Look For

### RFM Analysis
- Which personas are "Champions" (high value across all metrics)?
- Are "ultra_savers" consistently in high RFM segments?
- Do "wallet_users" show different RFM patterns than "savers"?

### Correlation Analysis
- Which demographics correlate most strongly with savings quality?
- Are there significant differences in age/gender/income distributions between personas?
- Which demographic groups are over-represented in "ultra_savers"?

## Output

Both scripts will generate:
1. **Console output** with key statistics and insights
2. **Visualizations** showing distributions and relationships
3. **Statistical test results** for validation

## Next Steps

After running these analyses, you can:
1. **Validate findings** with business stakeholders
2. **Create targeted marketing campaigns** based on persona-demographic insights
3. **Develop retention strategies** for different RFM segments
4. **Build predictive models** using the identified patterns

