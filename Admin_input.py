import mysql.connector

# Step 1: Connect to the database
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="project"
)

cursor = conn.cursor()

# Step 2: Get new user details from input
print("Enter new user access details:")
username = input("Username: ")
from_date = input("From Date (2011-01-03): ")
till_date = input("Till Date (2015-01-07): ")
region = input("restricted Region (Africa,Oceania,EMEA,North,Canada,Southeast Asia,Central,South,Caribbean,North Asia,East,West,Central Asia): ")
segment = input("restricted Segment (Consumer,Corporate,Home Office): ")
ship_mode = input("restricted Ship Mode (Standard Class,First Class,Same Day,Second Class): ")
password = input("Password: ")

# Step 3: Insert query with updated column names
insert_query = """
INSERT INTO user_access (
    username, from_date, till_date, region, segment, ship_mode, password
) VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

data = (
    username, from_date, till_date,
    region, segment, ship_mode, password
)

# Step 4: Execute and commit
try:
    cursor.execute(insert_query, data)
    conn.commit()
    print("✅ User added successfully!")
except Exception as e:
    print("❌ Error inserting data:", e)
finally:
    cursor.close()
    conn.close()
