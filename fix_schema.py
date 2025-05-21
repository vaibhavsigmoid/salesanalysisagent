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
    add_email_sql = "ALTER TABLE self_healing_demo ADD COLUMN email VARCHAR(255);"
    add_transaction_id_sql = "ALTER TABLE self_healing_demo ADD COLUMN transaction_id VARCHAR(50);"
    add_payment_method_sql = "ALTER TABLE self_healing_demo ADD COLUMN payment_method VARCHAR(50);"
    
    execute_sql(connection, add_email_sql)
    execute_sql(connection, add_transaction_id_sql)
    execute_sql(connection, add_payment_method_sql)

def alter_column_types(connection):
    alter_product_id_sql = "ALTER TABLE self_healing_demo MODIFY COLUMN product_id VARCHAR(50);"
    alter_alcohol_percentage_sql = "ALTER TABLE self_healing_demo MODIFY COLUMN alcohol_percentage FLOAT;"
    alter_unit_price_sql = "ALTER TABLE self_healing_demo MODIFY COLUMN unit_price FLOAT;"
    alter_total_sales_sql = "ALTER TABLE self_healing_demo MODIFY COLUMN total_sales FLOAT;"
    
    execute_sql(connection, alter_product_id_sql)
    execute_sql(connection, alter_alcohol_percentage_sql)
    execute_sql(connection, alter_unit_price_sql)
    execute_sql(connection, alter_total_sales_sql)

def cast_and_update_data(connection, csv_file_path):
    df = pd.read_csv(csv_file_path)
    
    df['alcohol_percentage'] = df['alcohol_percentage'].astype(float)
    df['unit_price'] = df['unit_price'].astype(float)
    df['total_sales'] = df['total_sales'].astype(float)
    
    for index, row in df.iterrows():
        update_sql = f"""
        UPDATE self_healing_demo
        SET product_id = %s,
            alcohol_percentage = %s,
            unit_price = %s,
            total_sales = %s,
            email = %s,
            transaction_id = %s,
            payment_method = %s
        WHERE sales_id = %s
        """
        values = (row['product_id'], row['alcohol_percentage'], row['unit_price'], row['total_sales'],
                  row['email'], row['transaction_id'], row['payment_method'], row['sales_id'])
        
        try:
            cursor = connection.cursor()
            cursor.execute(update_sql, values)
            connection.commit()
        except Error as e:
            print(f"Error updating row {index}: {e}")

def main():
    connection = connect_to_database()
    if connection is None:
        return
    
    add_new_columns(connection)
    alter_column_types(connection)
    cast_and_update_data(connection, '/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/self_healing/new_data.csv')
    
    connection.close()

if __name__ == "__main__":
    main()
