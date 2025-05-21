import pandas as pd
import mysql.connector
from mysql.connector import Error

def alter_target_schema():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='diageo_warehouse',
            user='root',
            password='password'
        )
        cursor = connection.cursor()

        # Add new columns
        cursor.execute("ALTER TABLE raw_pos ADD COLUMN email VARCHAR(255)")
        cursor.execute("ALTER TABLE raw_pos ADD COLUMN transaction_id VARCHAR(50)")
        cursor.execute("ALTER TABLE raw_pos ADD COLUMN payment_method VARCHAR(50)")

        # Modify column types
        cursor.execute("ALTER TABLE raw_pos MODIFY COLUMN product_id VARCHAR(50)")
        cursor.execute("ALTER TABLE raw_pos MODIFY COLUMN alcohol_percentage FLOAT")
        cursor.execute("ALTER TABLE raw_pos MODIFY COLUMN unit_price DECIMAL(10, 2)")
        cursor.execute("ALTER TABLE raw_pos MODIFY COLUMN total_sales DECIMAL(10, 2)")

        connection.commit()
    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def process_data(df):
    df['product_id'] = df['product_id'].astype(str)
    df['alcohol_percentage'] = pd.to_numeric(df['alcohol_percentage'], errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['total_sales'] = pd.to_numeric(df['total_sales'], errors='coerce')
    return df

def insert_data(df):
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='diageo_warehouse',
            user='root',
            password='password'
        )
        cursor = connection.cursor()

        for _, row in df.iterrows():
            sql = "INSERT INTO raw_pos (sales_id, product_id, store_id, date, customer_id, customer_name, gender, age, alcohol_percentage, product_type, product_category, product_line, city, quantity_sold, unit_price, total_sales, email, transaction_id, payment_method) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = tuple(row)
            cursor.execute(sql, values)

        connection.commit()
    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    alter_target_schema()
    df = pd.read_csv('/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/combined_testcase/raw_pos.csv')
    df = process_data(df)
    insert_data(df)
