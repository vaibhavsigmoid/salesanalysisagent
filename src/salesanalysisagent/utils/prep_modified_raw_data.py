import random
import string

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
NUM_RECORDS = 100  # Adjust as needed
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
payment_methods = ["Credit Card", "Cash", "Debit Card", "Mobile Payment"]


def _gen_id(length):
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


def generate_product_id(length=8):
    return _gen_id(length)


def generate_transaction_id(length=10):
    return _gen_id(length)


# Generate raw data
raw_sales_data = []
for i in range(1, NUM_RECORDS + 1):
    # product_id = random.randint(1, 100)
    product_id = (
        generate_product_id()
    )  ## changed the product_id from integer to alpha numberic
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
    sales_date = fake.date_between(start_date="-60d", end_date="today").strftime(
        "%Y/%m/%d"
    )  ## change date format
    quantity_sold = random.randint(1, 50)
    unit_price = round(random.uniform(5.0, 100.0), 2)
    total_sales = round(quantity_sold * unit_price, 2)

    # ✅ New columns
    email = fake.email()  ## additional column
    # customer_name = fake.name()
    transaction_id = generate_transaction_id()
    payment_method = random.choice(payment_methods)

    raw_sales_data.append(
        [
            i + 1000,
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
            email,
            transaction_id,
            payment_method,
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
        "email",
        "transaction_id",
        "payment_method",
    ],
)

# Save as CSV (optional)
df_raw.to_csv("raw_pos.csv", index=False)
print("CSV 'raw_pos.csv' generated.")
