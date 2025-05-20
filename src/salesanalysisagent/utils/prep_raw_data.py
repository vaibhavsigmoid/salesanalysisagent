import random

import mysql.connector
import pandas as pd
from faker import Faker

# Initialize Faker
fake = Faker()

# MySQL connection config
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "password",
    "database": "diageo_warehouse",
}

# Data generation parameters
NUM_RECORDS = 10  # Adjust as needed
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
regions = ["Northeast", "West", "Midwest", "South", "Southeast"]
store_types = ["Retail", "Wholesale"]
volumes = [500, 700, 750, 1000]

# Generate raw data
raw_sales_data = []
for i in range(1, NUM_RECORDS + 1):
    product_id = random.randint(101, 110)
    store_id = random.randint(101, 150)
    product_name = f"{random.choice(brands)} {fake.word().capitalize()}"
    brand = product_name.split()[0]
    category = random.choice(categories)
    volume_ml = random.choice(volumes)
    alcohol_percentage = round(random.uniform(4.0, 45.0), 2)
    store_name = fake.company()
    city = fake.city()
    state = fake.state_abbr()
    region = random.choice(regions)
    store_type = random.choice(store_types)
    sales_date = fake.date_between(start_date="-60d", end_date="today")
    quantity_sold = random.randint(1, 50)
    unit_price = round(random.uniform(5.0, 100.0), 2)
    total_sales = round(quantity_sold * unit_price, 2)

    raw_sales_data.append(
        [
            i + 10,
            product_id,
            store_id,
            product_name,
            brand,
            category,
            volume_ml,
            alcohol_percentage,
            store_name,
            city,
            state,
            region,
            store_type,
            sales_date,
            quantity_sold,
            unit_price,
            total_sales,
        ]
    )

# Create DataFrame (optional CSV export)
df_raw = pd.DataFrame(
    raw_sales_data,
    columns=[
        "sales_id",
        "product_id",
        "store_id",
        "product_name",
        "brand",
        "category",
        "volume_ml",
        "alcohol_percentage",
        "store_name",
        "city",
        "state",
        "region",
        "store_type",
        "sales_date",
        "quantity_sold",
        "unit_price",
        "total_sales",
    ],
)

# Save as CSV (optional)
df_raw.to_csv("pos_raw_sales.csv", index=False)
print("CSV 'pos_raw_sales.csv' generated.")

# Insert into MySQL
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

# Create table if not exists
cursor.execute("DROP TABLE IF EXISTS raw_pos")
cursor.execute(
    """
    CREATE TABLE raw_pos (
        sales_id INT,
        product_id INT,
        store_id INT,
        product_name VARCHAR(100),
        brand VARCHAR(50),
        category VARCHAR(50),
        volume_ml INT,
        alcohol_percentage DECIMAL(4,2),
        store_name VARCHAR(100),
        city VARCHAR(50),
        state VARCHAR(50),
        region VARCHAR(50),
        store_type VARCHAR(50),
        sales_date DATE,
        quantity_sold INT,
        unit_price DECIMAL(10,2),
        total_sales DECIMAL(10,2)
    )
"""
)

# Insert records
insert_query = """
    INSERT INTO raw_pos (
        sales_id, product_id, store_id, product_name, brand,
        category, volume_ml, alcohol_percentage, store_name,
        city, state, region, store_type, sales_date,
        quantity_sold, unit_price, total_sales
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
cursor.executemany(insert_query, raw_sales_data)
conn.commit()

cursor.close()
conn.close()

print("Data inserted into 'raw_pos' table in MySQL.")
