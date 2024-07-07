import pandas as pd
import numpy as np
from datetime import datetime
# Create a DataFrame
file_path= "transaction_data_with_anomalies.csv"
df = pd.read_csv(file_path)

### Cleaning data
# Remove or fill missing data

# Option 1: Remove rows with any missing values
df = df.dropna()

# Option 2: Fill missing values
# Fill missing dates with a specific date
# df = df.fillna({'date': '2024-01-01', 'category': 'Unknown', 'amount': df['amount'].mean()})


# Replace corrupt data

# Define a function to replace corrupt amount values
def replace_corrupt_values(amount):
    if amount < 0:  # condition for corrupt data
        return np.nan
    return amount
# Apply the function to the 'amount' column
df['amount'] = df['amount'].apply(replace_corrupt_values)
# Fill the newly created NaN values
df['amount'].fillna(df['amount'].mean(), inplace=True)

# Define a function to replace corrupt date values
def is_valid_date(date_string, date_format="%Y-%m-%d"):
    try:
        datetime.strptime(date_string, date_format)
        return True
    except ValueError:
        return False
# Apply the is_valid_date function to filter out invalid dates
df['is_valid_date'] = df['date'].apply(is_valid_date)

# Filter the DataFrame to keep only valid dates
df = df[df['is_valid_date']].drop(columns=['is_valid_date'])

# Convert date to datetime
df['date'] = pd.to_datetime(df['date'])

# Calculate basic statistical metrics
stats = df.groupby('category')['amount'].agg(['mean', 'median', 'std', 'count'])


# Establish thresholds for outlier detection
# Z-score method (threshold = 3 standard deviations)
z_threshold = 3
freq_threshold = 3


# IQR method
def calculate_iqr_bounds(df):
    Q1 = df['amount'].quantile(0.25)
    Q3 = df['amount'].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    return lower_bound, upper_bound


iqr_bounds = df.groupby('category').apply(calculate_iqr_bounds).apply(pd.Series)
iqr_bounds.columns = ['iqr_lower_bound', 'iqr_upper_bound']

# Merge stats and iqr_bounds for easy reference
stats = stats.merge(iqr_bounds, on='category')


# Function to detect anomalies
def detect_anomalies(df, stats, z_threshold):
    anomalies = []

    for index, row in df.iterrows():
        category = row['category']
        amount = row['amount']
        mean = stats.at[category, 'mean']
        std = stats.at[category, 'std']
        z_score = (amount - mean) / std if std != 0 else 0
        lower_bound, upper_bound = stats.at[category, 'iqr_lower_bound'], stats.at[category, 'iqr_upper_bound']

        if abs(z_score) > z_threshold or amount < lower_bound or amount > upper_bound:
            reason = []
            if abs(z_score) > z_threshold:
                reason.append(f"Z-score exceeds threshold = 3 standard deviations")
            if amount < lower_bound or amount > upper_bound:
                reason.append(f"Amount outside IQR bounds)")

            anomalies.append({
                "transaction_id": row['transaction_id'],
                "date": row['date'],
                "category": row['category'],
                "amount": row['amount'],
                "reason_for_anomaly": "; ".join(reason)
            })
    # Detect frequency anomalies
    daily_transaction_counts = df.groupby(['date', 'category']).size().reset_index(name='counts')
    category_means = daily_transaction_counts.groupby('category')['counts'].mean()
    category_stds = daily_transaction_counts.groupby('category')['counts'].std()
    for index, row in daily_transaction_counts.iterrows():
        category = row['category']
        counts = row['counts']
        mean = category_means[category]
        std = category_stds[category]
        z_score = (counts - mean) / std if std != 0 else 0

        if abs(z_score) > freq_threshold:
            reason = f"High frequency of transactions (Z-score {z_score:.2f}, threshold {freq_threshold})"
            anomalies.append({
                "transaction_id": None,  # No specific transaction ID for frequency anomalies
                "date": row['date'],
                "category": row['category'],
                "amount": None,
                "reason_for_anomaly": reason
            })

    return anomalies


# Detect anomalies
anomalies = detect_anomalies(df, stats, z_threshold)
if not anomalies:
    print("No anomalies present.")
    exit(0)
print(anomalies)
# Convert anomalies to DataFrame
anomalies_df = pd.DataFrame(anomalies)


# Function to generate a summary report
def generate_report(anomalies_df):

    summary = anomalies_df['reason_for_anomaly'].value_counts()
    report = f"Anomaly Detection Report\n{'=' * 30}\n\n"
    report += f"Total anomalies detected: {len(anomalies_df)}\n\n"
    report += "Summary of reasons for anomalies:\n"
    report += summary.to_string() + "\n\n"
    report += "Detailed list of anomalies:\n"
    report += anomalies_df.to_string(index=False)

    return report


# Generate and print the report
report = generate_report(anomalies_df)
print(report)
