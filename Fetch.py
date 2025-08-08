import mysql.connector
import pandas as pd

# Predefined queries
queries = {
    "sales_by_market": """
        SELECT market, SUM(sales) AS total_sales
        FROM orders
        GROUP BY market;
    """,
    "yearly_sales_2012": """
        SELECT market, ship_date, SUM(sales) AS total_sales
        FROM orders
        WHERE ship_date LIKE '%2012%'
        GROUP BY ROLLUP(market, ship_date);
    """,
    "top_customers": """
        SELECT customer_name, SUM(sales) AS total_sales
        FROM orders
        GROUP BY customer_name
        ORDER BY total_sales DESC
        LIMIT 10;
    """,
    "sales_by_segment": """
        SELECT segment, SUM(sales) AS total_sales
        FROM orders
        GROUP BY segment
        ORDER BY total_sales DESC;
    """,
    "top_products": """
        SELECT product_name, SUM(sales) AS total_sales
        FROM orders
        GROUP BY product_name
        ORDER BY total_sales DESC
        LIMIT 10;
    """,
    "ship_mode_count": """
        SELECT ship_mode, COUNT(*) AS shipment_count
        FROM orders
        GROUP BY ship_mode
        ORDER BY shipment_count DESC;
    """,
    "profit_vs_sales": """
        SELECT sales, profit
        FROM orders
        WHERE profit IS NOT NULL AND sales IS NOT NULL;
    """,
    "avg_order_value": """
        SELECT AVG(sales) AS average_order_value
        FROM orders;
    """,
    "delivery_time_vs_sales": """
        SELECT region,
               AVG(DATEDIFF(ship_date, order_date)) AS avg_delivery_days,
               SUM(sales) AS total_sales
        FROM orders
        WHERE ship_date IS NOT NULL AND order_date IS NOT NULL
        GROUP BY region
        ORDER BY avg_delivery_days DESC;
    """,
    "restricted": ""  # will be dynamically generated
}

def run_query(query_key, username=None):
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="password",
            database="project"
        )

        # Build query dynamically if it's restricted
        if query_key == "restricted":
            query = f"""
              SELECT o.*
                FROM orders o
                JOIN user_access u ON u.username = '{username}'
                WHERE NOT FIND_IN_SET(o.region, u.region)
                AND NOT FIND_IN_SET(o.segment, u.segment)
                AND NOT FIND_IN_SET(o.ship_mode, u.ship_mode)
                AND STR_TO_DATE(o.order_date, '%Y-%m-%d') NOT BETWEEN u.from_date AND u.till_date;
            """
        else:
            query = queries[query_key]

        df = pd.read_sql(query, conn)
        print(f"Rows returned: {len(df)}")

        # Save as CSV
        filename = f"{query_key}_{username}.csv" if username else f"{query_key}.csv"
        df.to_csv(filename, index=False)
        print(f"✅ Saved to {filename}")

    except mysql.connector.Error as err:
        print(f"❌ Database error: {err}")
    finally:
        if conn.is_connected():
            conn.close()

# Show available queries
print("\nAvailable Queries:")
for i, q in enumerate(queries.keys(), start=1):
    print(f"{i}. {q}")

# User input
choice = input("\nEnter query name to run: ").strip()

# Restricted query requires username
if choice == "restricted":
    uname = input("Enter username: ").strip()
    run_query(choice, username=uname)
else:
    run_query(choice)