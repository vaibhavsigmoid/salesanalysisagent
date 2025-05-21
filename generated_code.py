
# Output from schema_validator_task (Schema validator)
import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging
import os
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

def get_database_connection():
    """Establish and return a database connection."""
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME')
        )
        return connection
    except Error as e:
        logging.error(f"Error connecting to MySQL database: {e}")
        return None

def execute_sql(connection, sql):
    """Execute SQL statement and handle errors."""
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            connection.commit()
        logging.info(f"SQL executed successfully: {sql}")
    except Error as e:
        logging.error(f"Error executing SQL: {e}")
        connection.rollback()

def add_email_column(connection):
    """Add email column to the raw_pos table."""
    sql = "ALTER TABLE raw_pos ADD COLUMN email VARCHAR(255);"
    execute_sql(connection, sql)

def alter_product_id_column(connection):
    """Alter product_id column to VARCHAR in the raw_pos table."""
    sql = "ALTER TABLE raw_pos MODIFY COLUMN product_id VARCHAR(255);"
    execute_sql(connection, sql)

def convert_product_id(df):
    """Convert product_id column to string type."""
    df['product_id'] = df['product_id'].astype(str)
    return df

def update_database_with_dataframe(connection, df, table_name):
    """Update the database table with the dataframe using batch inserts."""
    try:
        cursor = connection.cursor()
        # Prepare the INSERT statement
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        # Convert DataFrame to list of tuples
        values = [tuple(row) for row in df.to_numpy()]
        
        # Execute batch insert
        cursor.executemany(sql, values)
        connection.commit()
        logging.info(f"Successfully inserted {len(df)} rows into {table_name}")
    except Error as e:
        logging.error(f"Error updating database: {e}")
        connection.rollback()
    finally:
        cursor.close()

def main():
    # Establish database connection
    connection = get_database_connection()
    if not connection:
        return

    try:
        # Add email column
        add_email_column(connection)

        # Alter product_id column
        alter_product_id_column(connection)

        # Read CSV file
        df = pd.read_csv('/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/combined_testcase/raw_pos.csv')

        # Convert product_id to string
        df = convert_product_id(df)

        # Clean and validate data
        df = df.dropna()  # Remove rows with missing values
        df['email'] = df['email'].str.lower()  # Convert email to lowercase

        # Update database with cleaned data
        update_database_with_dataframe(connection, df, 'raw_pos')

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if connection.is_connected():
            connection.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    main()

# Output from schema_validator_task (Schema validator)
import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging
import os
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

def get_database_connection():
    """
    Establish and return a connection to the MySQL database.
    """
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME')
        )
        if connection.is_connected():
            logging.info('Successfully connected to the database')
            return connection
    except Error as e:
        logging.error(f'Error connecting to the database: {e}')
        return None

def close_connection(connection):
    """
    Close the database connection.
    """
    if connection.is_connected():
        connection.close()
        logging.info('Database connection closed')

def add_email_column(connection):
    """
    Add the 'email' column to the raw_pos table.
    """
    try:
        cursor = connection.cursor()
        add_column_query = "ALTER TABLE raw_pos ADD COLUMN email VARCHAR(255)"
        cursor.execute(add_column_query)
        connection.commit()
        logging.info("'email' column added successfully")
    except Error as e:
        logging.error(f"Error adding 'email' column: {e}")
        connection.rollback()
    finally:
        cursor.close()

def update_product_id_column(connection):
    """
    Update the 'product_id' column to VARCHAR(255) in the raw_pos table.
    """
    try:
        cursor = connection.cursor()
        alter_column_query = "ALTER TABLE raw_pos MODIFY COLUMN product_id VARCHAR(255)"
        cursor.execute(alter_column_query)
        connection.commit()
        logging.info("'product_id' column type updated successfully")
    except Error as e:
        logging.error(f"Error updating 'product_id' column type: {e}")
        connection.rollback()
    finally:
        cursor.close()

def process_and_insert_data(connection, csv_file_path):
    """
    Process the CSV file and insert data into the raw_pos table.
    """
    try:
        # Read CSV file
        df = pd.read_csv(csv_file_path)
        
        # Ensure product_id is treated as string
        df['product_id'] = df['product_id'].astype(str)
        
        # Prepare data for insertion
        data_to_insert = df.to_dict('records')
        
        cursor = connection.cursor()
        
        # Prepare the INSERT query
        insert_query = """
        INSERT INTO raw_pos (sales_id, product_id, store_id, customer_id, sales_date, sales_time, 
                             price_per_unit, price_modifier, quantity_sold, unit_price, total_sales, email)
        VALUES (%(sales_id)s, %(product_id)s, %(store_id)s, %(customer_id)s, %(sales_date)s, %(sales_time)s, 
                %(price_per_unit)s, %(price_modifier)s, %(quantity_sold)s, %(unit_price)s, %(total_sales)s, %(email)s)
        ON DUPLICATE KEY UPDATE
        product_id = VALUES(product_id),
        store_id = VALUES(store_id),
        customer_id = VALUES(customer_id),
        sales_date = VALUES(sales_date),
        sales_time = VALUES(sales_time),
        price_per_unit = VALUES(price_per_unit),
        price_modifier = VALUES(price_modifier),
        quantity_sold = VALUES(quantity_sold),
        unit_price = VALUES(unit_price),
        total_sales = VALUES(total_sales),
        email = VALUES(email)
        """
        
        # Execute batch insert
        cursor.executemany(insert_query, data_to_insert)
        connection.commit()
        logging.info(f"{cursor.rowcount} records inserted successfully")
    except Error as e:
        logging.error(f"Error inserting data: {e}")
        connection.rollback()
    finally:
        cursor.close()

def main():
    # Establish database connection
    connection = get_database_connection()
    if not connection:
        return
    
    try:
        # Add 'email' column
        add_email_column(connection)
        
        # Update 'product_id' column type
        update_product_id_column(connection)
        
        # Process and insert data
        csv_file_path = '/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/combined_testcase/raw_pos.csv'
        process_and_insert_data(connection, csv_file_path)
        
        logging.info("Schema update and data insertion completed successfully")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        close_connection(connection)

if __name__ == "__main__":
    main()

# Output from schema_validator_task (Schema validator)
import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection parameters
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'database': os.environ.get('DB_NAME', 'diageo_warehouse'),
    'user': os.environ.get('DB_USER', 'your_username'),
    'password': os.environ.get('DB_PASSWORD', 'your_password')
}

def connect_to_database():
    """Establish a connection to the MySQL database."""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            logging.info('Connected to MySQL database')
        return connection
    except Error as e:
        logging.error(f'Error connecting to MySQL database: {e}')
        return None

def close_connection(connection):
    """Close the database connection."""
    if connection.is_connected():
        connection.close()
        logging.info('Database connection closed')

def execute_query(connection, query):
    """Execute a SQL query and commit changes."""
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        logging.info('Query executed successfully')
    except Error as e:
        logging.error(f'Error executing query: {e}')
        connection.rollback()
    finally:
        cursor.close()

def add_email_column(connection):
    """Add email column to the raw_pos table."""
    query = """ALTER TABLE raw_pos
               ADD COLUMN email VARCHAR(255);"""
    execute_query(connection, query)

def alter_product_id_type(connection):
    """Alter product_id column type to VARCHAR(255)."""
    query = """ALTER TABLE raw_pos
               MODIFY COLUMN product_id VARCHAR(255);"""
    execute_query(connection, query)

def cast_and_update_data(connection, csv_file):
    """Cast and update data for alcohol_percentage, unit_price, and total_sales."""
    df = pd.read_csv(csv_file)
    
    # Ensure data types
    df['alcohol_percentage'] = pd.to_numeric(df['alcohol_percentage'], errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['total_sales'] = pd.to_numeric(df['total_sales'], errors='coerce')
    
    # Update database in batches
    batch_size = 1000
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        placeholders = ', '.join(['%s'] * len(batch.columns))
        columns = ', '.join(batch.columns)
        query = f"""INSERT INTO raw_pos ({columns}) VALUES ({placeholders})
                   ON DUPLICATE KEY UPDATE
                   alcohol_percentage = VALUES(alcohol_percentage),
                   unit_price = VALUES(unit_price),
                   total_sales = VALUES(total_sales),
                   email = VALUES(email);"""
        
        values = [tuple(row) for row in batch.values]
        
        try:
            cursor = connection.cursor()
            cursor.executemany(query, values)
            connection.commit()
            logging.info(f'Batch {i//batch_size + 1} updated successfully')
        except Error as e:
            logging.error(f'Error updating batch {i//batch_size + 1}: {e}')
            connection.rollback()
        finally:
            cursor.close()

def main():
    """Main function to execute the schema fix and data update process."""
    connection = connect_to_database()
    if not connection:
        return
    
    try:
        add_email_column(connection)
        alter_product_id_type(connection)
        
        csv_file = '/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/combined_testcase/raw_pos.csv'
        cast_and_update_data(connection, csv_file)
        
        logging.info('Schema fix and data update completed successfully')
    except Exception as e:
        logging.error(f'An error occurred: {e}')
    finally:
        close_connection(connection)

if __name__ == '__main__':
    main()

# Output from schema_validator_task (Schema validator)
import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to create a database connection
def create_db_connection():
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME')
        )
        return connection
    except Error as e:
        logging.error(f"Error connecting to MySQL database: {e}")
        return None

# Function to execute SQL queries
def execute_query(connection, query):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        logging.info("Query executed successfully")
    except Error as e:
        logging.error(f"Error executing query: {e}")

# Function to add new columns to the target table
def add_new_columns(connection):
    add_columns_query = """
    ALTER TABLE raw_pos
    ADD COLUMN email VARCHAR(255),
    ADD COLUMN transaction_id VARCHAR(50),
    ADD COLUMN payment_method VARCHAR(50);
    """
    execute_query(connection, add_columns_query)

# Function to alter column types in the target table
def alter_column_types(connection):
    alter_columns_query = """
    ALTER TABLE raw_pos
    MODIFY COLUMN product_id VARCHAR(50),
    MODIFY COLUMN alcohol_percentage DECIMAL(5,2),
    MODIFY COLUMN unit_price DECIMAL(10,2),
    MODIFY COLUMN total_sales DECIMAL(10,2);
    """
    execute_query(connection, alter_columns_query)

# Function to cast and clean data
def cast_and_clean_data(df):
    # Convert product_id to string
    df['product_id'] = df['product_id'].astype(str)
    
    # Convert alcohol_percentage, unit_price, and total_sales to float
    for col in ['alcohol_percentage', 'unit_price', 'total_sales']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Remove any potential invalid characters from email, transaction_id, and payment_method
    for col in ['email', 'transaction_id', 'payment_method']:
        df[col] = df[col].astype(str).str.replace('[^\w@.-]', '', regex=True)
    
    return df

# Function to insert data into the target table
def insert_data(connection, df):
    cursor = connection.cursor()
    
    # Prepare the INSERT statement
    insert_query = """
    INSERT INTO raw_pos (sales_id, product_id, store_id, customer_id, sales_date, 
                         quantity_sold, alcohol_percentage, brand, category, 
                         subcategory, volume_ml, unit_price, total_sales, email, 
                         transaction_id, payment_method)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Convert DataFrame to list of tuples
    data = [tuple(row) for row in df.values]
    
    try:
        # Use executemany for batch insert
        cursor.executemany(insert_query, data)
        connection.commit()
        logging.info(f"Inserted {len(data)} rows into raw_pos table")
    except Error as e:
        logging.error(f"Error inserting data: {e}")
    finally:
        cursor.close()

def main():
    # Create database connection
    connection = create_db_connection()
    if not connection:
        return
    
    try:
        # Add new columns to the target table
        add_new_columns(connection)
        
        # Alter column types in the target table
        alter_column_types(connection)
        
        # Read the source CSV file
        df = pd.read_csv('/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/combined_testcase/raw_pos.csv')
        
        # Cast and clean the data
        df = cast_and_clean_data(df)
        
        # Insert the data into the target table
        insert_data(connection, df)
        
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if connection.is_connected():
            connection.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    main()

# Output from schema_validator_task (Schema validator)
import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging
import os
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

def get_database_connection():
    """Establish and return a database connection."""
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        if connection.is_connected():
            logging.info('Successfully connected to the database')
            return connection
    except Error as e:
        logging.error(f'Error connecting to the database: {e}')
        return None

def close_database_connection(connection):
    """Close the database connection."""
    if connection.is_connected():
        connection.close()
        logging.info('Database connection closed')

def execute_sql(connection, sql):
    """Execute SQL statement and handle errors."""
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        connection.commit()
        logging.info(f'SQL executed successfully: {sql}')
    except Error as e:
        logging.error(f'Error executing SQL: {e}')
        connection.rollback()

def add_new_columns(connection):
    """Add new columns to the target table."""
    new_columns = [
        "ALTER TABLE raw_pos ADD COLUMN email VARCHAR(255)",
        "ALTER TABLE raw_pos ADD COLUMN transaction_id VARCHAR(50)",
        "ALTER TABLE raw_pos ADD COLUMN payment_method VARCHAR(50)"
    ]
    for sql in new_columns:
        execute_sql(connection, sql)

def alter_column_types(connection):
    """Alter column types in the target table."""
    alter_statements = [
        "ALTER TABLE raw_pos MODIFY COLUMN product_id VARCHAR(50)",
        "ALTER TABLE raw_pos MODIFY COLUMN alcohol_percentage FLOAT",
        "ALTER TABLE raw_pos MODIFY COLUMN unit_price DECIMAL(10, 2)",
        "ALTER TABLE raw_pos MODIFY COLUMN total_sales DECIMAL(10, 2)"
    ]
    for sql in alter_statements:
        execute_sql(connection, sql)

def convert_data_types(df):
    """Convert data types in the DataFrame to match target schema."""
    df['product_id'] = df['product_id'].astype(str)
    df['alcohol_percentage'] = pd.to_numeric(df['alcohol_percentage'], errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['total_sales'] = pd.to_numeric(df['total_sales'], errors='coerce')
    return df

def update_database(connection, df):
    """Update the database with the processed DataFrame."""
    cursor = connection.cursor()
    for _, row in df.iterrows():
        sql = """INSERT INTO raw_pos (sales_id, product_id, store_id, product_name, brand, product_type, 
                 variety, alcohol_percentage, volume_ml, quantity_sold, unit_price, total_sales, sale_date, 
                 store_name, store_location, email, transaction_id, payment_method) 
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        product_id = VALUES(product_id),
        store_id = VALUES(store_id),
        product_name = VALUES(product_name),
        brand = VALUES(brand),
        product_type = VALUES(product_type),
        variety = VALUES(variety),
        alcohol_percentage = VALUES(alcohol_percentage),
        volume_ml = VALUES(volume_ml),
        quantity_sold = VALUES(quantity_sold),
        unit_price = VALUES(unit_price),
        total_sales = VALUES(total_sales),
        sale_date = VALUES(sale_date),
        store_name = VALUES(store_name),
        store_location = VALUES(store_location),
        email = VALUES(email),
        transaction_id = VALUES(transaction_id),
        payment_method = VALUES(payment_method)"""
        values = tuple(row)
        cursor.execute(sql, values)
    connection.commit()
    logging.info(f'{cursor.rowcount} records inserted or updated successfully.')

def main():
    # Establish database connection
    connection = get_database_connection()
    if not connection:
        return

    try:
        # Add new columns
        add_new_columns(connection)

        # Alter column types
        alter_column_types(connection)

        # Read and process CSV file
        df = pd.read_csv('/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/combined_testcase/raw_pos.csv')
        df = convert_data_types(df)

        # Update database
        update_database(connection, df)

    except Exception as e:
        logging.error(f'An error occurred: {e}')
    finally:
        close_database_connection(connection)

if __name__ == "__main__":
    main()

# Output from schema_validator_task (Schema validator)
import pandas as pd
import mysql.connector
from mysql.connector import Error

def fix_schema():
    # SQL statements to add new columns
    add_columns_sql = """
    ALTER TABLE raw_pos
    ADD COLUMN email VARCHAR(255),
    ADD COLUMN transaction_id VARCHAR(50),
    ADD COLUMN payment_method VARCHAR(50);
    """

    # SQL statements to alter column types
    alter_columns_sql = """
    ALTER TABLE raw_pos
    MODIFY COLUMN product_id VARCHAR(50),
    MODIFY COLUMN alcohol_percentage FLOAT,
    MODIFY COLUMN unit_price DECIMAL(10, 2),
    MODIFY COLUMN total_sales DECIMAL(10, 2);
    """

    # Function to cast data types
    def cast_datatypes(df):
        df['product_id'] = df['product_id'].astype(str)
        df['alcohol_percentage'] = df['alcohol_percentage'].astype(float)
        df['unit_price'] = df['unit_price'].astype(float)
        df['total_sales'] = df['total_sales'].astype(float)
        return df

    # Connect to the MySQL database
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='diageo_warehouse',
            user='your_username',
            password='your_password'
        )

        if connection.is_connected():
            cursor = connection.cursor()

            # Execute SQL statements to add new columns
            cursor.execute(add_columns_sql)

            # Execute SQL statements to alter column types
            cursor.execute(alter_columns_sql)

            connection.commit()

            # Read the CSV file
            df = pd.read_csv('/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/combined_testcase/raw_pos.csv')

            # Cast data types
            df = cast_datatypes(df)

            # Insert data into the database
            for _, row in df.iterrows():
                insert_query = f"""INSERT INTO raw_pos 
                    (sales_id, product_id, store_id, product_name, brand, category, volume_ml, alcohol_percentage, state, region, store_type, sales_date, quantity_sold, unit_price, total_sales, email, transaction_id, payment_method)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cursor.execute(insert_query, tuple(row))

            connection.commit()
            print("Schema fixed and data inserted successfully.")

    except Error as e:
        print(f"Error: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    fix_schema()

# Output from schema_validator_task (Schema validator)
import pandas as pd
import mysql.connector
from mysql.connector import Error

def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host='your_host',
            database='diageo_warehouse',
            user='your_username',
            password='your_password'
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
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
    ADD COLUMN transaction_id VARCHAR(20),
    ADD COLUMN payment_method VARCHAR(50);
    """
    execute_sql(connection, add_columns_sql)

def modify_column_types(connection):
    modify_columns_sql = """
    ALTER TABLE raw_pos
    MODIFY COLUMN product_id VARCHAR(20),
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

# Output from schema_validator_task (Schema validator)
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
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
    return None

def execute_sql(connection, sql):
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        connection.commit()
    except Error as e:
        print(f"Error executing SQL: {e}")

def add_new_columns(connection):
    add_column_sql = """
    ALTER TABLE raw_pos
    ADD COLUMN email VARCHAR(255),
    ADD COLUMN transaction_id VARCHAR(20),
    ADD COLUMN payment_method VARCHAR(50);
    """
    execute_sql(connection, add_column_sql)

def alter_column_types(connection):
    alter_column_sql = """
    ALTER TABLE raw_pos
    MODIFY COLUMN product_id VARCHAR(20),
    MODIFY COLUMN alcohol_percentage FLOAT,
    MODIFY COLUMN unit_price DECIMAL(10, 2),
    MODIFY COLUMN total_sales DECIMAL(10, 2);
    """
    execute_sql(connection, alter_column_sql)

def cast_and_update_data(df, connection):
    df['product_id'] = df['product_id'].astype(str)
    df['alcohol_percentage'] = pd.to_numeric(df['alcohol_percentage'], errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['total_sales'] = pd.to_numeric(df['total_sales'], errors='coerce')

    cursor = connection.cursor()
    for index, row in df.iterrows():
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

def main():
    connection = connect_to_database()
    if connection:
        add_new_columns(connection)
        alter_column_types(connection)
        
        df = pd.read_csv('/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/combined_testcase/raw_pos.csv')
        cast_and_update_data(df, connection)
        
        connection.close()

if __name__ == "__main__":
    main()

# Output from schema_validator_task (Schema validator)
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

def add_new_columns(connection):
    cursor = connection.cursor()
    try:
        cursor.execute("ALTER TABLE raw_pos ADD COLUMN email VARCHAR(255)")
        cursor.execute("ALTER TABLE raw_pos ADD COLUMN transaction_id VARCHAR(20)")
        cursor.execute("ALTER TABLE raw_pos ADD COLUMN payment_method VARCHAR(50)")
        connection.commit()
        print("New columns added successfully")
    except Error as e:
        print(f"Error adding new columns: {e}")
    finally:
        cursor.close()

def alter_column_types(connection):
    cursor = connection.cursor()
    try:
        cursor.execute("ALTER TABLE raw_pos MODIFY COLUMN product_id VARCHAR(20)")
        cursor.execute("ALTER TABLE raw_pos MODIFY COLUMN alcohol_percentage DECIMAL(5,2)")
        cursor.execute("ALTER TABLE raw_pos MODIFY COLUMN unit_price DECIMAL(10,2)")
        cursor.execute("ALTER TABLE raw_pos MODIFY COLUMN total_sales DECIMAL(10,2)")
        connection.commit()
        print("Column types altered successfully")
    except Error as e:
        print(f"Error altering column types: {e}")
    finally:
        cursor.close()

def cast_and_update_data(csv_file_path, connection):
    df = pd.read_csv(csv_file_path)
    
    df['product_id'] = df['product_id'].astype(str)
    df['alcohol_percentage'] = pd.to_numeric(df['alcohol_percentage'], errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['total_sales'] = pd.to_numeric(df['total_sales'], errors='coerce')
    
    cursor = connection.cursor()
    try:
        for _, row in df.iterrows():
            update_query = """
            UPDATE raw_pos
            SET product_id = %s,
                alcohol_percentage = %s,
                unit_price = %s,
                total_sales = %s,
                email = %s,
                transaction_id = %s,
                payment_method = %s
            WHERE sales_id = %s
            """
            cursor.execute(update_query, (
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
        print("Data updated successfully")
    except Error as e:
        print(f"Error updating data: {e}")
    finally:
        cursor.close()

def main():
    connection = connect_to_database()
    if connection is not None:
        add_new_columns(connection)
        alter_column_types(connection)
        cast_and_update_data('/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/combined_testcase/raw_pos.csv', connection)
        connection.close()

if __name__ == "__main__":
    main()

# Output from schema_validator_task (Schema validator)
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

def modify_target_schema(connection):
    # Add new columns
    add_columns_sql = """
    ALTER TABLE raw_pos
    ADD COLUMN email VARCHAR(255),
    ADD COLUMN transaction_id VARCHAR(50),
    ADD COLUMN payment_method VARCHAR(50);
    """
    execute_sql(connection, add_columns_sql)

    # Modify column types
    modify_columns_sql = """
    ALTER TABLE raw_pos
    MODIFY COLUMN product_id VARCHAR(50),
    MODIFY COLUMN alcohol_percentage FLOAT,
    MODIFY COLUMN unit_price DECIMAL(10, 2),
    MODIFY COLUMN total_sales DECIMAL(10, 2);
    """
    execute_sql(connection, modify_columns_sql)

def cast_and_insert_data(csv_file_path, connection):
    df = pd.read_csv(csv_file_path)
    
    # Cast data types
    df['product_id'] = df['product_id'].astype(str)
    df['alcohol_percentage'] = pd.to_numeric(df['alcohol_percentage'], errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['total_sales'] = pd.to_numeric(df['total_sales'], errors='coerce')
    
    # Insert data into the table
    cursor = connection.cursor()
    for _, row in df.iterrows():
        insert_sql = """
        INSERT INTO raw_pos (sales_id, product_id, store_id, product_name, category, subcategory, brand, sale_date, quantity_sold, unit_price, total_sales, alcohol_percentage, email, transaction_id, payment_method)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        product_id = VALUES(product_id),
        store_id = VALUES(store_id),
        product_name = VALUES(product_name),
        category = VALUES(category),
        subcategory = VALUES(subcategory),
        brand = VALUES(brand),
        sale_date = VALUES(sale_date),
        quantity_sold = VALUES(quantity_sold),
        unit_price = VALUES(unit_price),
        total_sales = VALUES(total_sales),
        alcohol_percentage = VALUES(alcohol_percentage),
        email = VALUES(email),
        transaction_id = VALUES(transaction_id),
        payment_method = VALUES(payment_method);
        """
        cursor.execute(insert_sql, tuple(row))
    connection.commit()

def main():
    connection = connect_to_database()
    if connection:
        modify_target_schema(connection)
        cast_and_insert_data('/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/combined_testcase/raw_pos.csv', connection)
        connection.close()

if __name__ == "__main__":
    main()

# Output from schema_validator_task (Schema validator)
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
        print(f'Error connecting to MySQL database: {e}')
        return None

def execute_sql(connection, sql):
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        connection.commit()
    except Error as e:
        print(f'Error executing SQL: {e}')

def add_new_columns(connection):
    add_columns_sql = """
    ALTER TABLE raw_pos
    ADD COLUMN email VARCHAR(255),
    ADD COLUMN transaction_id VARCHAR(20),
    ADD COLUMN payment_method VARCHAR(50);
    """
    execute_sql(connection, add_columns_sql)

def modify_column_types(connection):
    modify_columns_sql = """
    ALTER TABLE raw_pos
    MODIFY COLUMN product_id VARCHAR(20),
    MODIFY COLUMN alcohol_percentage DECIMAL(5,2),
    MODIFY COLUMN unit_price DECIMAL(10,2),
    MODIFY COLUMN total_sales DECIMAL(12,2);
    """
    execute_sql(connection, modify_columns_sql)

def cast_and_update_data(connection, csv_file_path):
    df = pd.read_csv(csv_file_path)
    
    df['product_id'] = df['product_id'].astype(str)
    df['alcohol_percentage'] = pd.to_numeric(df['alcohol_percentage'], errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['total_sales'] = pd.to_numeric(df['total_sales'], errors='coerce')
    
    for index, row in df.iterrows():
        update_sql = f"""
        UPDATE raw_pos
        SET product_id = '{row['product_id']}',
            alcohol_percentage = {row['alcohol_percentage']},
            unit_price = {row['unit_price']},
            total_sales = {row['total_sales']},
            email = '{row['email']}',
            transaction_id = '{row['transaction_id']}',
            payment_method = '{row['payment_method']}'
        WHERE sales_id = {row['sales_id']};
        """
        execute_sql(connection, update_sql)

def main():
    connection = connect_to_database()
    if connection:
        add_new_columns(connection)
        modify_column_types(connection)
        cast_and_update_data(connection, '/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/combined_testcase/raw_pos.csv')
        connection.close()
    else:
        print('Failed to connect to the database')

if __name__ == '__main__':
    main()

# Output from schema_validator_task (Schema validator)
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

def alter_table_schema(connection):
    # Add new columns
    add_column_sql = """
    ALTER TABLE raw_pos
    ADD COLUMN email VARCHAR(255),
    ADD COLUMN transaction_id VARCHAR(50),
    ADD COLUMN payment_method VARCHAR(50);
    """
    execute_sql(connection, add_column_sql)

    # Alter column types
    alter_column_sql = """
    ALTER TABLE raw_pos
    MODIFY COLUMN product_id VARCHAR(50),
    MODIFY COLUMN alcohol_percentage FLOAT,
    MODIFY COLUMN unit_price DECIMAL(10, 2),
    MODIFY COLUMN total_sales DECIMAL(10, 2);
    """
    execute_sql(connection, alter_column_sql)

def cast_and_load_data(csv_file_path, connection):
    df = pd.read_csv(csv_file_path)

    # Cast data types
    df['product_id'] = df['product_id'].astype(str)
    df['alcohol_percentage'] = pd.to_numeric(df['alcohol_percentage'], errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['total_sales'] = pd.to_numeric(df['total_sales'], errors='coerce')

    # Load data into the database
    cursor = connection.cursor()
    for _, row in df.iterrows():
        insert_sql = """
        INSERT INTO raw_pos (
            sales_id, product_id, store_id, sale_date, quantity_sold, unit_price, total_sales,
            customer_id, customer_name, gender, age, region, product_name, product_category,
            manufacturer, alcohol_percentage, email, transaction_id, payment_method
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            product_id = VALUES(product_id),
            store_id = VALUES(store_id),
            sale_date = VALUES(sale_date),
            quantity_sold = VALUES(quantity_sold),
            unit_price = VALUES(unit_price),
            total_sales = VALUES(total_sales),
            customer_id = VALUES(customer_id),
            customer_name = VALUES(customer_name),
            gender = VALUES(gender),
            age = VALUES(age),
            region = VALUES(region),
            product_name = VALUES(product_name),
            product_category = VALUES(product_category),
            manufacturer = VALUES(manufacturer),
            alcohol_percentage = VALUES(alcohol_percentage),
            email = VALUES(email),
            transaction_id = VALUES(transaction_id),
            payment_method = VALUES(payment_method)
        """
        cursor.execute(insert_sql, tuple(row))
    connection.commit()

def main():
    connection = connect_to_database()
    if connection:
        alter_table_schema(connection)
        cast_and_load_data('/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/combined_testcase/raw_pos.csv', connection)
        connection.close()

if __name__ == "__main__":
    main()

# Output from schema_validator_task (Schema validator)
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

def alter_column_types(connection):
    alter_columns_sql = """
    ALTER TABLE raw_pos
    MODIFY COLUMN product_id VARCHAR(50),
    MODIFY COLUMN alcohol_percentage FLOAT,
    MODIFY COLUMN unit_price DECIMAL(10, 2),
    MODIFY COLUMN total_sales DECIMAL(10, 2);
    """
    execute_sql(connection, alter_columns_sql)

def cast_and_update_data(connection, csv_file_path):
    df = pd.read_csv(csv_file_path)
    
    df['product_id'] = df['product_id'].astype(str)
    df['alcohol_percentage'] = pd.to_numeric(df['alcohol_percentage'], errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['total_sales'] = pd.to_numeric(df['total_sales'], errors='coerce')
    
    cursor = connection.cursor()
    for index, row in df.iterrows():
        update_sql = """
        UPDATE raw_pos
        SET 
            product_id = %s,
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

def main():
    connection = connect_to_database()
    if connection:
        add_new_columns(connection)
        alter_column_types(connection)
        cast_and_update_data(connection, '/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/combined_testcase/raw_pos.csv')
        connection.close()

if __name__ == "__main__":
    main()

# Output from schema_validator_task (Schema validator)
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
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
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
    MODIFY COLUMN alcohol_percentage FLOAT,
    MODIFY COLUMN unit_price DECIMAL(10, 2),
    MODIFY COLUMN total_sales DECIMAL(10, 2);
    """
    execute_sql(connection, modify_columns_sql)

def cast_and_update_data(csv_file_path, connection):
    df = pd.read_csv(csv_file_path)
    
    df['product_id'] = df['product_id'].astype(str)
    df['alcohol_percentage'] = pd.to_numeric(df['alcohol_percentage'], errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['total_sales'] = pd.to_numeric(df['total_sales'], errors='coerce')
    
    cursor = connection.cursor()
    
    for index, row in df.iterrows():
        update_sql = """
        UPDATE raw_pos
        SET 
            product_id = %s,
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

# Output from schema_validator_task (Schema validator)
import pandas as pd
import mysql.connector
from mysql.connector import Error

def fix_schema():
    # Database connection parameters
    db_config = {
        'host': 'localhost',
        'database': 'diageo_warehouse',
        'user': 'root',
        'password': 'password'
    }

    try:
        # Connect to the MySQL database
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Add new columns
        cursor.execute("ALTER TABLE raw_pos ADD COLUMN email VARCHAR(255)")
        cursor.execute("ALTER TABLE raw_pos ADD COLUMN transaction_id VARCHAR(255)")
        cursor.execute("ALTER TABLE raw_pos ADD COLUMN payment_method VARCHAR(255)")

        # Modify column types
        cursor.execute("ALTER TABLE raw_pos MODIFY COLUMN product_id VARCHAR(255)")
        cursor.execute("ALTER TABLE raw_pos MODIFY COLUMN alcohol_percentage FLOAT")
        cursor.execute("ALTER TABLE raw_pos MODIFY COLUMN unit_price FLOAT")
        cursor.execute("ALTER TABLE raw_pos MODIFY COLUMN total_sales FLOAT")

        conn.commit()

        # Read CSV file
        df = pd.read_csv('/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/combined_testcase/raw_pos.csv')

        # Cast columns to appropriate types
        df['product_id'] = df['product_id'].astype(str)
        df['alcohol_percentage'] = df['alcohol_percentage'].astype(float)
        df['unit_price'] = df['unit_price'].astype(float)
        df['total_sales'] = df['total_sales'].astype(float)

        # Insert data into the table
        for _, row in df.iterrows():
            sql = "INSERT INTO raw_pos (sales_id, product_id, store_id, customer_id, purchase_date, quantity_sold, unit_price, total_sales, alcohol_percentage, email, transaction_id, payment_method) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (row['sales_id'], row['product_id'], row['store_id'], row['customer_id'], row['purchase_date'], row['quantity_sold'], row['unit_price'], row['total_sales'], row['alcohol_percentage'], row['email'], row['transaction_id'], row['payment_method'])
            cursor.execute(sql, values)

        conn.commit()

    except Error as e:
        print(f"Error: {e}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    fix_schema()

# Output from schema_validator_task (Schema validator)
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
        if connection.is_connected():
            return connection
    except Error as e:
        print(f'Error connecting to MySQL database: {e}')
    return None

def execute_sql(connection, sql):
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        connection.commit()
    except Error as e:
        print(f'Error executing SQL: {e}')

def add_new_columns():
    connection = connect_to_database()
    if connection:
        sql_add_columns = """
        ALTER TABLE raw_pos
        ADD COLUMN email VARCHAR(255),
        ADD COLUMN transaction_id VARCHAR(50),
        ADD COLUMN payment_method VARCHAR(50);
        """
        execute_sql(connection, sql_add_columns)
        connection.close()

def fix_column_types():
    connection = connect_to_database()
    if connection:
        sql_alter_types = """
        ALTER TABLE raw_pos
        MODIFY COLUMN product_id VARCHAR(50),
        MODIFY COLUMN alcohol_percentage FLOAT,
        MODIFY COLUMN unit_price DECIMAL(10, 2),
        MODIFY COLUMN total_sales DECIMAL(10, 2);
        """
        execute_sql(connection, sql_alter_types)
        connection.close()

def cast_and_update_data(csv_file_path):
    df = pd.read_csv(csv_file_path)
    
    df['product_id'] = df['product_id'].astype(str)
    df['alcohol_percentage'] = pd.to_numeric(df['alcohol_percentage'], errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['total_sales'] = pd.to_numeric(df['total_sales'], errors='coerce')
    
    connection = connect_to_database()
    if connection:
        cursor = connection.cursor()
        for index, row in df.iterrows():
            update_sql = """
            UPDATE raw_pos
            SET 
                product_id = %s,
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
        connection.close()

if __name__ == '__main__':
    add_new_columns()
    fix_column_types()
    cast_and_update_data('/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/combined_testcase/raw_pos.csv')

# Output from schema_validator_task (Schema validator)
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

def alter_table_schema(connection):
    # Add new columns
    add_columns_sql = """
    ALTER TABLE raw_pos
    ADD COLUMN email VARCHAR(255),
    ADD COLUMN transaction_id VARCHAR(50),
    ADD COLUMN payment_method VARCHAR(50);
    """
    execute_sql(connection, add_columns_sql)

    # Modify column types
    modify_columns_sql = """
    ALTER TABLE raw_pos
    MODIFY COLUMN product_id VARCHAR(50),
    MODIFY COLUMN alcohol_percentage DECIMAL(5,2),
    MODIFY COLUMN unit_price DECIMAL(10,2),
    MODIFY COLUMN total_sales DECIMAL(10,2);
    """
    execute_sql(connection, modify_columns_sql)

def cast_and_load_data(csv_file_path, connection):
    df = pd.read_csv(csv_file_path)

    # Cast columns to appropriate types
    df['product_id'] = df['product_id'].astype(str)
    df['alcohol_percentage'] = pd.to_numeric(df['alcohol_percentage'], errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['total_sales'] = pd.to_numeric(df['total_sales'], errors='coerce')

    # Load data into the database
    cursor = connection.cursor()
    for _, row in df.iterrows():
        insert_sql = """
        INSERT INTO raw_pos (sales_id, product_id, store_id, date, category, subcategory, product_name, brand, variant, alcohol_percentage, volume, quantity_sold, unit_price, total_sales, email, transaction_id, payment_method)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        product_id = VALUES(product_id),
        store_id = VALUES(store_id),
        date = VALUES(date),
        category = VALUES(category),
        subcategory = VALUES(subcategory),
        product_name = VALUES(product_name),
        brand = VALUES(brand),
        variant = VALUES(variant),
        alcohol_percentage = VALUES(alcohol_percentage),
        volume = VALUES(volume),
        quantity_sold = VALUES(quantity_sold),
        unit_price = VALUES(unit_price),
        total_sales = VALUES(total_sales),
        email = VALUES(email),
        transaction_id = VALUES(transaction_id),
        payment_method = VALUES(payment_method);
        """
        cursor.execute(insert_sql, tuple(row))
    connection.commit()

def main():
    connection = connect_to_database()
    if connection:
        alter_table_schema(connection)
        cast_and_load_data('/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/combined_testcase/raw_pos.csv', connection)
        connection.close()

if __name__ == "__main__":
    main()

# Output from schema_validator_task (Schema validator)
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
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
    return None

def add_new_columns(connection):
    cursor = connection.cursor()
    try:
        cursor.execute("ALTER TABLE raw_pos ADD COLUMN email VARCHAR(255)")
        cursor.execute("ALTER TABLE raw_pos ADD COLUMN transaction_id VARCHAR(50)")
        cursor.execute("ALTER TABLE raw_pos ADD COLUMN payment_method VARCHAR(50)")
        connection.commit()
        print("New columns added successfully.")
    except Error as e:
        print(f"Error adding new columns: {e}")
    finally:
        cursor.close()

def convert_data_types(df):
    df['product_id'] = df['product_id'].astype(int)
    df['alcohol_percentage'] = pd.to_numeric(df['alcohol_percentage'], errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['total_sales'] = pd.to_numeric(df['total_sales'], errors='coerce')
    return df

def update_target_table(connection, df):
    cursor = connection.cursor()
    try:
        for index, row in df.iterrows():
            update_query = """
            UPDATE raw_pos
            SET product_id = %s,
                alcohol_percentage = %s,
                unit_price = %s,
                total_sales = %s,
                email = %s,
                transaction_id = %s,
                payment_method = %s
            WHERE sales_id = %s
            """
            cursor.execute(update_query, (
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
        print("Target table updated successfully.")
    except Error as e:
        print(f"Error updating target table: {e}")
    finally:
        cursor.close()

def main():
    connection = connect_to_database()
    if connection:
        add_new_columns(connection)
        df = pd.read_csv('/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/combined_testcase/raw_pos.csv')
        df = convert_data_types(df)
        update_target_table(connection, df)
        connection.close()

if __name__ == "__main__":
    main()

# Output from schema_validator_task (Schema validator)
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
        if connection.is_connected():
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
    ADD COLUMN transaction_id VARCHAR(20),
    ADD COLUMN payment_method VARCHAR(50);
    """
    execute_sql(connection, add_columns_sql)

def modify_column_types(connection):
    modify_columns_sql = """
    ALTER TABLE raw_pos
    MODIFY COLUMN product_id VARCHAR(20),
    MODIFY COLUMN alcohol_percentage DECIMAL(5,2),
    MODIFY COLUMN unit_price DECIMAL(10,2),
    MODIFY COLUMN total_sales DECIMAL(10,2);
    """
    execute_sql(connection, modify_columns_sql)

def cast_and_insert_data(csv_file_path, connection):
    df = pd.read_csv(csv_file_path)
    
    df['product_id'] = df['product_id'].astype(str)
    df['alcohol_percentage'] = pd.to_numeric(df['alcohol_percentage'], errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['total_sales'] = pd.to_numeric(df['total_sales'], errors='coerce')
    
    cursor = connection.cursor()
    for _, row in df.iterrows():
        insert_sql = """
        INSERT INTO raw_pos (sales_id, product_id, store_id, customer_id, category, brand, product_name, alcohol_percentage, quantity_sold, unit_price, total_sales, sales_date, email, transaction_id, payment_method)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        product_id = VALUES(product_id),
        store_id = VALUES(store_id),
        customer_id = VALUES(customer_id),
        category = VALUES(category),
        brand = VALUES(brand),
        product_name = VALUES(product_name),
        alcohol_percentage = VALUES(alcohol_percentage),
        quantity_sold = VALUES(quantity_sold),
        unit_price = VALUES(unit_price),
        total_sales = VALUES(total_sales),
        sales_date = VALUES(sales_date),
        email = VALUES(email),
        transaction_id = VALUES(transaction_id),
        payment_method = VALUES(payment_method);
        """
        cursor.execute(insert_sql, tuple(row))
    connection.commit()
    cursor.close()

def main():
    connection = connect_to_database()
    if connection:
        add_new_columns(connection)
        modify_column_types(connection)
        cast_and_insert_data('/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/combined_testcase/raw_pos.csv', connection)
        connection.close()
    else:
        print("Failed to connect to the database.")

if __name__ == "__main__":
    main()

# Output from schema_validator_task (Schema validator)
import pandas as pd
import mysql.connector
from mysql.connector import Error

def fix_schema_and_data():
    # MySQL connection parameters
    config = {
        'user': 'root',
        'password': 'password',
        'host': 'localhost',
        'database': 'diageo_warehouse',
        'raise_on_warnings': True
    }

    try:
        # Establish MySQL connection
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()

        # Alter table to change column types
        alter_queries = [
            "ALTER TABLE raw_pos MODIFY COLUMN alcohol_percentage FLOAT;",
            "ALTER TABLE raw_pos MODIFY COLUMN unit_price FLOAT;",
            "ALTER TABLE raw_pos MODIFY COLUMN total_sales FLOAT;"
        ]

        for query in alter_queries:
            cursor.execute(query)

        conn.commit()

        # Read the source CSV file
        df = pd.read_csv('/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/combined_testcase/raw_pos.csv')

        # Ensure data types are correct
        df['alcohol_percentage'] = df['alcohol_percentage'].astype(float)
        df['unit_price'] = df['unit_price'].astype(float)
        df['total_sales'] = df['total_sales'].astype(float)

        # Update the data in the MySQL table
        for index, row in df.iterrows():
            update_query = """
            UPDATE raw_pos
            SET alcohol_percentage = %s, unit_price = %s, total_sales = %s
            WHERE sales_id = %s
            """
            cursor.execute(update_query, (row['alcohol_percentage'], row['unit_price'], row['total_sales'], row['sales_id']))

        conn.commit()
        print("Schema and data updated successfully.")

    except Error as e:
        print(f"Error: {e}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    fix_schema_and_data()

# Output from schema_validator_task (Schema validator)
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

# Output from schema_validator_task (Schema validator)
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
