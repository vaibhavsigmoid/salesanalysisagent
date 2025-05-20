import random

import mysql.connector
import pandas as pd
from faker import Faker

fake = Faker()

# Define constants
num_products = 50
num_stores = 20
num_sales = 500  # Adjust this number for more rows

# Generate production_dim
product_ids = range(1, num_products + 1)
brands = [
    "Johnnie Walker",
    "Guinness",
    "Smirnoff",
    "Baileys",
    "Captain Morgan",
    "Ciroc",
    "Tanqueray",
    "Don Julio",
]
categories = ["Whisky", "Beer", "Vodka", "Liqueur", "Rum", "Gin", "Tequila"]

production_dim = pd.DataFrame(
    {
        "product_id": product_ids,
        "product_name": [
            f"{random.choice(brands)} {fake.word().capitalize()}" for _ in product_ids
        ],
        "brand": [random.choice(brands) for _ in product_ids],
        "category": [random.choice(categories) for _ in product_ids],
        "volume_ml": [random.choice([500, 700, 750, 1000]) for _ in product_ids],
        "alcohol_percentage": [
            round(random.uniform(4.0, 45.0), 2) for _ in product_ids
        ],
    }
)

# Generate store_dim
store_ids = range(101, 101 + num_stores)
regions = ["Northeast", "West", "Midwest", "South", "Southeast"]
store_types = ["Retail", "Wholesale"]

store_dim = pd.DataFrame(
    {
        "store_id": store_ids,
        "store_name": [fake.company() for _ in store_ids],
        "city": [fake.city() for _ in store_ids],
        "state": [fake.state_abbr() for _ in store_ids],
        "region": [random.choice(regions) for _ in store_ids],
        "store_type": [random.choice(store_types) for _ in store_ids],
    }
)

# Generate sales_fact
sales_fact = []
for i in range(num_sales):
    product_id = random.choice(product_ids)
    store_id = random.choice(store_ids)
    sales_date = fake.date_between(start_date="-90d", end_date="today")
    quantity_sold = random.randint(1, 50)
    unit_price = round(random.uniform(5.0, 100.0), 2)
    total_sales = round(quantity_sold * unit_price, 2)
    sales_fact.append(
        [
            i + 1,
            product_id,
            store_id,
            sales_date,
            quantity_sold,
            unit_price,
            total_sales,
        ]
    )

sales_fact_df = pd.DataFrame(
    sales_fact,
    columns=[
        "sales_id",
        "product_id",
        "store_id",
        "sales_date",
        "quantity_sold",
        "unit_price",
        "total_sales",
    ],
)

# Export to CSV
production_dim.to_csv("production_dim.csv", index=False)
store_dim.to_csv("store_dim.csv", index=False)
sales_fact_df.to_csv("sales_fact.csv", index=False)


DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "password",
    "database": "diageo_warehouse",
}

conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

# Create tables
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS production_dim (
    product_id INT,
    product_name VARCHAR(100),
    brand VARCHAR(50),
    category VARCHAR(50),
    volume_ml INT,
    alcohol_percentage DECIMAL(4,2)
)
"""
)

cursor.execute(
    """
CREATE TABLE IF NOT EXISTS store_dim (
    store_id INT,
    store_name VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50),
    region VARCHAR(50),
    store_type VARCHAR(50)
)
"""
)

cursor.execute(
    """
CREATE TABLE IF NOT EXISTS sales_fact (
    sales_id INT,
    product_id INT,
    store_id INT,
    sales_date DATE,
    quantity_sold INT,
    unit_price DECIMAL(10,2),
    total_sales DECIMAL(10,2)
)
"""
)

# Insert data
PROD_QUERY = "INSERT INTO production_dim VALUES (%s, %s, %s, %s, %s, %s)"
STORE_QUERY = "INSERT INTO store_dim VALUES (%s, %s, %s, %s, %s, %s)"
SALES_QUERY = "INSERT INTO sales_fact VALUES (%s, %s, %s, %s, %s, %s, %s)"

cursor.executemany(PROD_QUERY, production_dim.values.tolist())
cursor.executemany(STORE_QUERY, store_dim.values.tolist())
cursor.executemany(SALES_QUERY, sales_fact)

conn.commit()
cursor.close()
conn.close()
