import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Parameters
num_entries = 1050
start_date = datetime(2024, 1, 1)
categories = ['Food', 'Utilities', 'Entertainment', 'Transport', 'Health', 'Shopping']
customer_ids = np.arange(1, 11)  # 10 customers

# Generate random transaction data
np.random.seed(42)
data = []
for i in range(num_entries):
    transaction_id = f"TRX{i+1:04d}"
    date = start_date + timedelta(days=np.random.randint(0, 200))
    category = np.random.choice(categories)
    amount = np.random.uniform(10, 100)  # Regular transaction amounts

    # Inject anomalies
    if i % 50 == 0:  # High transaction amount anomaly
        amount = np.random.uniform(1000, 5000)
    if 500 < i < 510:  # Sudden frequency increase anomaly
        date = start_date + timedelta(days=i-500)
        amount = np.random.uniform(10, 100)
    if i % 70 == 0 and i != 0:  # Irregular pattern anomaly
        amount = np.random.uniform(300, 700)

    data.append([transaction_id, date.strftime('%Y-%m-%d'), category, round(amount, 2)])

# Create DataFrame
transactions_df = pd.DataFrame(data, columns=['transaction_id', 'date', 'category', 'amount'])

# Save to CSV
file_path = '/mnt/data/transaction_data_with_anomalies.csv'
transactions_df.to_csv(file_path, index=False)