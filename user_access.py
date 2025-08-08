import mysql.connector
import pandas as pd

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="project"
  
)

query = """
SELECT o.*
FROM orders o
JOIN user_access u ON u.username = 'gautam'
WHERE NOT FIND_IN_SET(o.region, u.region)
  AND NOT FIND_IN_SET(o.segment, u.segment)
  AND NOT FIND_IN_SET(o.ship_mode, u.ship_mode)
  AND STR_TO_DATE(o.order_date, '%Y-%m-%d') NOT BETWEEN u.from_date AND u.till_date;
"""

df = pd.read_sql(query, conn)
conn.close()
df.to_csv('gautam_filtered_orders.csv', index=False)


print(df.head())
print(df.columns)

import matplotlib.pyplot as plt
import pandas as pd

# Group by Month
df['order_date'] = pd.to_datetime(df['order_date'])
df['sales'] = pd.to_numeric(df['sales'], errors='coerce')
df['month'] = df['order_date'].dt.to_period('M').astype(str)

sales_by_month = df.groupby('month')['sales'].sum()

# Plot
plt.figure(figsize=(12, 6))
plt.bar(sales_by_month.index, sales_by_month.values, color='skyblue')
plt.title('Total Sales by Month')
plt.xlabel('Month')
plt.ylabel('Sales')
plt.xticks(rotation=45)
plt.tight_layout()
plt.grid(axis='y')
plt.show()

import seaborn as sns

plt.figure(figsize=(8, 5))
sns.barplot(data=df, x='region', y='sales', estimator=sum)
plt.title('Sales by Region')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

plt.figure(figsize=(8, 5))
sns.boxplot(data=df, x='segment', y='sales')
plt.title('Sales Distribution by Segment')
plt.tight_layout()
plt.show()

plt.figure(figsize=(6, 4))
sns.countplot(data=df, x='ship_mode')
plt.title('Number of Orders by Ship Mode')
plt.tight_layout()
plt.show()

df['sales'] = pd.to_numeric(df['sales'], errors='coerce')
df['profit'] = pd.to_numeric(df['profit'], errors='coerce')
# Create 'quarter' column
df['quarter'] = df['order_date'].dt.to_period('Q').astype(str)
quarterly = df.groupby('quarter')[['sales', 'profit']].sum().reset_index()
# Plot
plt.figure(figsize=(12, 6))
plt.plot(quarterly['quarter'], quarterly['sales'], label='Sales', marker='o')
plt.plot(quarterly['quarter'], quarterly['profit'], label='Profit', marker='o')

plt.title('Quarterly Sales vs Profit')
plt.xlabel('Quarter')
plt.ylabel('Amount')
plt.xticks(rotation=45)
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()