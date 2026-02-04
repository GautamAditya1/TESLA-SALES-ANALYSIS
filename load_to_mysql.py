import mysql.connector
import pandas as pd

# Load CSV
df = pd.read_csv("TESLA_SALES_UPDATED.csv")

# Remove index column if exists
if 'Unnamed: 0' in df.columns:
    df.drop(columns=['Unnamed: 0'], inplace=True)




# Replace NaN with None
df = df.where(pd.notnull(df), None)

# MySQL connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root"
)

cursor = conn.cursor()

# Create DB
cursor.execute("CREATE DATABASE IF NOT EXISTS TESLA_SALES")
cursor.execute("USE TESLA_SALES")

# Create table (13 columns)
cursor.execute("""
CREATE TABLE IF NOT EXISTS sales (
    Year INT,
    Month INT,
    Region VARCHAR(50),
    Model VARCHAR(50),
    Estimated_Deliveries INT,
    Production_Units INT,
    Avg_Price_USD DECIMAL(10,2),
    Battery_Capacity_kWh INT,
    Range_km INT,
    CO2_Saved_tons DECIMAL(10,2),
    Source_Type VARCHAR(50),
    Charging_Stations INT,
    Revenue_Million_USD DECIMAL(12,2)
)
""")

# Insert query (13 columns)
insert_query = """
INSERT INTO sales (
    Year, Month, Region, Model, Estimated_Deliveries,
    Production_Units, Avg_Price_USD, Battery_Capacity_kWh,
    Range_km, CO2_Saved_tons, Source_Type,
    Charging_Stations, Revenue_Million_USD
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

data = list(df.itertuples(index=False, name=None))

cursor.executemany(insert_query, data)

conn.commit()
cursor.close()
conn.close()

print("All 13 columns inserted successfully into MySQL")
