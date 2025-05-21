import pandas as pd
import mysql.connector
from mysql.connector import Error

def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='diageo_warehouse',
            user='root',
            password='password'
        )
        return connection
    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None

def execute_sql(connection, sql):
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        connection.commit()
    except Error as e:
        print(f"Error executing SQL: {e}")

def add_new_columns(connection):
    add_columns_sql = """
    ALTER TABLE raw_pos
    ADD COLUMN email VARCHAR(255),
    ADD COLUMN transaction_id VARCHAR(50),
    ADD COLUMN payment_method VARCHAR(50);
    """
    execute_sql(connection, add_columns_sql)

def modify_column_types(connection):
    modify_columns_sql = """
    ALTER TABLE raw_pos
    MODIFY COLUMN product_id VARCHAR(50),
    MODIFY COLUMN alcohol_percentage DECIMAL(5,2),
    MODIFY COLUMN unit_price DECIMAL(10,2),
    MODIFY COLUMN total_sales DECIMAL(10,2);
    """
    execute_sql(connection, modify_columns_sql)

def cast_and_update_data(csv_file_path, connection):
    df = pd.read_csv(csv_file_path)
    
    df['product_id'] = df['product_id'].astype(str)
    df['alcohol_percentage'] = pd.to_numeric(df['alcohol_percentage'], errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['total_sales'] = pd.to_numeric(df['total_sales'], errors='coerce')
    
    cursor = connection.cursor()
    
    for _, row in df.iterrows():
        update_sql = """
        UPDATE raw_pos
        SET product_id = %s,
            alcohol_percentage = %s,
            unit_price = %s,
            total_sales = %s,
            email = %s,
            transaction_id = %s,
            payment_method = %s
        WHERE sales_id = %s;
        """
        cursor.execute(update_sql, (
            row['product_id'],
            row['alcohol_percentage'],
            row['unit_price'],
            row['total_sales'],
            row['email'],
            row['transaction_id'],
            row['payment_method'],
            row['sales_id']
        ))
    
    connection.commit()
    cursor.close()

def main():
    connection = connect_to_database()
    if connection:
        add_new_columns(connection)
        modify_column_types(connection)
        cast_and_update_data('/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/combined_testcase/raw_pos.csv', connection)
        connection.close()

if __name__ == "__main__":
    main()
