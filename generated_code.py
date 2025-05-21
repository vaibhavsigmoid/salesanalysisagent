Here's the customized Python code for the ETL pipeline to process the sales data:

```python
import pandas as pd
from datetime import datetime

def load_and_process_sales_data(input_file_path, output_file_path):
    # Load the data
    df = pd.read_csv(input_file_path)

    # Clean and transform the data
    df = clean_and_transform_data(df)

    # Save the processed data
    df.to_csv(output_file_path, index=False)
    print(f"Processed data saved to {output_file_path}")

def clean_and_transform_data(df):
    # Ensure product_id is treated as string
    df['product_id'] = df['product_id'].astype(str)

    # Convert price to float (in case it's not already)
    df['price'] = df['price'].astype(float)

    # Add a 'sales_date' column with the current date
    df['sales_date'] = datetime.now().strftime('%Y-%m-%d')

    # Format price as currency
    df['formatted_price'] = df['price'].apply(lambda x: f'${x:.2f}')

    # Calculate total sales (assuming we have a quantity column, if not, we can add it)
    if 'quantity' not in df.columns:
        df['quantity'] = 1  # Assume quantity of 1 if not provided
    df['total_sales'] = df['price'] * df['quantity']

    # Reorder and select final columns
    final_columns = ['product_id', 'product_desc', 'price', 'formatted_price', 'quantity', 'total_sales', 'sales_date']
    df = df[final_columns]

    return df

if __name__ == "__main__":
    input_file_path = "path_to_input_sales_data.csv"
    output_file_path = "processed_sales_data.csv"
    load_and_process_sales_data(input_file_path, output_file_path)
```

This ETL pipeline does the following:

1. Loads the sales data from a CSV file.
2. Cleans and transforms the data:
   - Ensures `product_id` is treated as a string to accommodate alphanumeric values.
   - Converts `price` to float for calculations.
   - Adds a `sales_date` column with the current date.
   - Formats the price as currency (e.g., $10.00).
   - Calculates total sales (assuming a quantity of 1 if not provided).
3. Reorders and selects the final columns for output.
4. Saves the processed data to a new CSV file.

To use this script:

1. Replace `"path_to_input_sales_data.csv"` with the actual path to your input CSV file.
2. Run the script, and it will generate a new CSV file named `processed_sales_data.csv` with the cleaned and transformed data.

This pipeline can be easily integrated into a larger system or scheduled to run periodically to process new sales data. It handles the current data structure, including alphanumeric product IDs, and prepares the data for further analysis or integration into the sales system.
# Output from data_loader_task (Data Loader and File Type Inspector)
{'error': 'Unsupported file type: .txt'}

# Output from inspect_task (Data Inspector)
Preview of data:
  product_id product_desc  price
0          1          aaa   10.0
1          2          bbb   30.0
2          3          ccc   20.0
3         3A          yyy   20.0
4         3B          zzz   20.0

Schema:
{'product_id': 'object', 'product_desc': 'object', 'price': 'float64'}

# Output from schema_mapping_task (Schema Mapper)
product_id  price
0          1   10.0
1          2   30.0
2          3   20.0
3         3A   20.0
4         3B   20.0

# Output from schema_validator_task (Schema validator)
```python
import pandas as pd
import mysql.connector
from mysql.connector import Error

def alter_product_id_column():
    try:
        connection = mysql.connector.connect(
            host='your_host',
            database='pos_data',
            user='your_username',
            password='your_password'
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Alter the product_id column to VARCHAR(10) to accommodate alphanumeric values
            alter_query = """
            ALTER TABLE sales_table
            MODIFY COLUMN product_id VARCHAR(10);
            """
            cursor.execute(alter_query)
            connection.commit()
            print("product_id column type altered successfully.")
            
    except Error as e:
        print(f"Error: {e}")
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def update_sales_table(df):
    try:
        connection = mysql.connector.connect(
            host='your_host',
            database='pos_data',
            user='your_username',
            password='your_password'
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Update existing rows and insert new ones
            for index, row in df.iterrows():
                update_query = """
                INSERT INTO sales_table (product_id, product_desc, price)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                product_desc = VALUES(product_desc),
                price = VALUES(price);
                """
                cursor.execute(update_query, (row['product_id'], row['product_desc'], row['price']))
            
            connection.commit()
            print("Sales table updated successfully.")
            
    except Error as e:
        print(f"Error: {e}")
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Main execution
if __name__ == "__main__":
    # Alter the product_id column to accommodate alphanumeric values
    alter_product_id_column()
    
    # Read the source CSV file
    df = pd.read_csv("/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/schema_change/sales.txt")
    
    # Update the sales table with new data
    update_sales_table(df)
```

# Output from data_format_check_task (Data Format Change Detector)
```python
import pandas as pd
from datetime import datetime

def fix_data_format_change(source_df):
    # Create a copy of the source dataframe to avoid modifying the original
    df = source_df.copy()
    
    # Convert price to string with '$' prefix and two decimal places
    df['price'] = df['price'].apply(lambda x: f'${x:.2f}')
    
    # Add a 'date' column with the current date
    current_date = datetime.now().strftime('%d-%m-%Y')
    df['date'] = current_date
    
    # Ensure product_id is treated as string
    df['product_id'] = df['product_id'].astype(str)
    
    # Reorder columns to match target schema
    df = df[['product_id', 'product_desc', 'price', 'date']]
    
    return df
```

This `fix_data_format_change` function does the following:

1. Creates a copy of the source dataframe to avoid modifying the original data.
2. Converts the 'price' column to a string with a '$' prefix and ensures two decimal places.
3. Adds a 'date' column with the current date (as we don't have this information in the source data).
4. Ensures the 'product_id' column is treated as a string to accommodate alphanumeric values.
5. Reorders the columns to match the target schema.

This function can be used to prepare the source data for ingestion into the existing table. It handles the differences in data format without dropping any data. The resulting dataframe will have the same structure as the target table, allowing for seamless appending of new data.

# Output from clean_validate_task (Data Cleaner and Validator)
product_id product_desc  price
0          1          aaa   10.0
1          2          bbb   30.0
2          3          ccc   20.0
3         3A          yyy   20.0
4         3B          zzz   20.0

This is the cleaned and validated dataframe. The data has been processed by the CleanValidateTool, and no issues were found. The dataframe maintains the original structure with three columns: product_id, product_desc, and price. The product_id column contains both numeric and alphanumeric values, the product_desc column contains short string descriptions, and the price column contains numeric values with one decimal place. This dataframe is now ready for further analysis or integration into the sales system.

# Output from code_gen_task (Code Generator)
Here's the customized Python code for the ETL pipeline to process the sales data:

```python
import pandas as pd
from datetime import datetime

def load_and_process_sales_data(input_file_path, output_file_path):
    # Load the data
    df = pd.read_csv(input_file_path)

    # Clean and transform the data
    df = clean_and_transform_data(df)

    # Save the processed data
    df.to_csv(output_file_path, index=False)
    print(f"Processed data saved to {output_file_path}")

def clean_and_transform_data(df):
    # Ensure product_id is treated as string
    df['product_id'] = df['product_id'].astype(str)

    # Convert price to float (in case it's not already)
    df['price'] = df['price'].astype(float)

    # Add a 'sales_date' column with the current date
    df['sales_date'] = datetime.now().strftime('%Y-%m-%d')

    # Format price as currency
    df['formatted_price'] = df['price'].apply(lambda x: f'${x:.2f}')

    # Calculate total sales (assuming we have a quantity column, if not, we can add it)
    if 'quantity' not in df.columns:
        df['quantity'] = 1  # Assume quantity of 1 if not provided
    df['total_sales'] = df['price'] * df['quantity']

    # Reorder and select final columns
    final_columns = ['product_id', 'product_desc', 'price', 'formatted_price', 'quantity', 'total_sales', 'sales_date']
    df = df[final_columns]

    return df

if __name__ == "__main__":
    input_file_path = "path_to_input_sales_data.csv"
    output_file_path = "processed_sales_data.csv"
    load_and_process_sales_data(input_file_path, output_file_path)
```

This ETL pipeline does the following:

1. Loads the sales data from a CSV file.
2. Cleans and transforms the data:
   - Ensures `product_id` is treated as a string to accommodate alphanumeric values.
   - Converts `price` to float for calculations.
   - Adds a `sales_date` column with the current date.
   - Formats the price as currency (e.g., $10.00).
   - Calculates total sales (assuming a quantity of 1 if not provided).
3. Reorders and selects the final columns for output.
4. Saves the processed data to a new CSV file.

To use this script:

1. Replace `"path_to_input_sales_data.csv"` with the actual path to your input CSV file.
2. Run the script, and it will generate a new CSV file named `processed_sales_data.csv` with the cleaned and transformed data.

This pipeline can be easily integrated into a larger system or scheduled to run periodically to process new sales data. It handles the current data structure, including alphanumeric product IDs, and prepares the data for further analysis or integration into the sales system.

# Output from schema_validator_task (Schema validator)
```python
import pandas as pd
import sqlite3
from typing import List, Tuple

def handle_schema_changes(source_file: str, target_table: str, db_path: str) -> List[str]:
    # Read the source CSV file
    df_source = pd.read_csv(source_file)
    
    # Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get the target table schema
    cursor.execute(f"PRAGMA table_info({target_table})")
    target_schema = {row[1]: row[2] for row in cursor.fetchall()}
    
    changes = []
    
    # Compare source and target schemas
    for column in df_source.columns:
        if column not in target_schema:
            # New column in source, add to target
            data_type = get_sqlite_type(df_source[column].dtype)
            changes.append(f"ALTER TABLE {target_table} ADD COLUMN {column} {data_type};")
        elif target_schema[column] != get_sqlite_type(df_source[column].dtype):
            # Column type mismatch, alter target column
            new_type = get_sqlite_type(df_source[column].dtype)
            changes.append(f"ALTER TABLE {target_table} ALTER COLUMN {column} {new_type};")
    
    # Check for columns in target that are not in source
    for column in target_schema:
        if column not in df_source.columns:
            # Column in target but not in source, keep it to preserve historical data
            pass
    
    # Close the database connection
    conn.close()
    
    return changes

def get_sqlite_type(pandas_dtype) -> str:
    if pd.api.types.is_integer_dtype(pandas_dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(pandas_dtype):
        return "REAL"
    elif pd.api.types.is_datetime64_any_dtype(pandas_dtype):
        return "DATETIME"
    else:
        return "TEXT"

def cast_columns(df: pd.DataFrame, target_schema: dict) -> pd.DataFrame:
    for column, dtype in target_schema.items():
        if column in df.columns:
            if dtype == "INTEGER":
                df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
            elif dtype == "REAL":
                df[column] = pd.to_numeric(df[column], errors='coerce').astype('float64')
            elif dtype == "DATETIME":
                df[column] = pd.to_datetime(df[column], errors='coerce')
            else:
                df[column] = df[column].astype(str)
    return df

def update_database(db_path: str, target_table: str, source_file: str) -> None:
    # Get schema changes
    changes = handle_schema_changes(source_file, target_table, db_path)
    
    # Apply schema changes
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    for change in changes:
        cursor.execute(change)
    conn.commit()
    
    # Get updated target schema
    cursor.execute(f"PRAGMA table_info({target_table})")
    target_schema = {row[1]: row[2] for row in cursor.fetchall()}
    
    # Read and cast source data
    df_source = pd.read_csv(source_file)
    df_source = cast_columns(df_source, target_schema)
    
    # Insert new data
    df_source.to_sql(target_table, conn, if_exists='append', index=False)
    
    conn.close()

# Usage
db_path = "path/to/your/database.db"
target_table = "sales"
source_file = "/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/schema_change/sales.txt"

update_database(db_path, target_table, source_file)
```

This Python code provides a comprehensive solution to handle schema changes between the source CSV file and the target SQLite database table. Here's what the code does:

1. The `handle_schema_changes` function compares the source and target schemas, generating SQL statements to add new columns or alter existing ones if there are type mismatches.

2. The `get_sqlite_type` function maps pandas data types to SQLite data types.

3. The `cast_columns` function ensures that the data types in the source DataFrame match the target schema, handling type conversions and potential errors.

4. The `update_database` function ties everything together:
   - It gets and applies schema changes.
   - It reads the source file and casts the columns to match the target schema.
   - It appends the new data to the existing table.

This approach ensures that:
- New columns in the source are added to the target table.
- Existing columns with type mismatches are altered in the target table.
- Columns in the target that are not in the source are preserved, maintaining historical data.
- Data is cast to the appropriate types before insertion, preventing data loss or truncation.

The code is designed to be flexible and can handle various schema change scenarios while preserving data integrity.

# Output from schema_validator_task (Schema validator)
```python
import pandas as pd
import mysql.connector
from mysql.connector import Error

def cast_price_to_decimal(df):
    """
    Cast the 'price' column to Decimal(10, 2) to match the target schema.
    """
    df['price'] = df['price'].astype('float64').round(2)
    return df

def alter_price_column_type():
    """
    Alter the 'price' column type in the target database to DECIMAL(10, 2).
    """
    try:
        connection = mysql.connector.connect(
            host='your_host',
            database='pos_data',
            user='your_username',
            password='your_password'
        )
        
        cursor = connection.cursor()
        
        alter_query = """
        ALTER TABLE sales_table
        MODIFY COLUMN price DECIMAL(10, 2);
        """
        
        cursor.execute(alter_query)
        connection.commit()
        print("Column 'price' type altered successfully.")
        
    except Error as e:
        print(f"Error altering column type: {e}")
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def update_sales_data(df):
    """
    Update the sales_table with the new data, ensuring proper type conversion.
    """
    try:
        connection = mysql.connector.connect(
            host='your_host',
            database='pos_data',
            user='your_username',
            password='your_password'
        )
        
        cursor = connection.cursor()
        
        for _, row in df.iterrows():
            update_query = """
            INSERT INTO sales_table (product_id, product_desc, price)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
            product_desc = VALUES(product_desc),
            price = VALUES(price);
            """
            cursor.execute(update_query, (row['product_id'], row['product_desc'], row['price']))
        
        connection.commit()
        print("Sales data updated successfully.")
        
    except Error as e:
        print(f"Error updating sales data: {e}")
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Main execution
df = pd.read_csv('/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/schema_change/sales.txt')
df = cast_price_to_decimal(df)
alter_price_column_type()
update_sales_data(df)
```

# Output from schema_validator_task (Schema validator)
```python
import re

def transform_price(price):
    """
    Transform price values by removing 'USD' and converting to DECIMAL(10,2)
    """
    return float(re.sub(r'[^\d.]', '', price))

def update_price_column():
    """
    Update the price column in the target table
    """
    # SQL to alter the price column to DECIMAL(10,2)
    alter_price_sql = """
    ALTER TABLE pos_data.sales_table
    MODIFY COLUMN price DECIMAL(10,2);
    """
    
    # SQL to update existing rows
    update_price_sql = """
    UPDATE pos_data.sales_table
    SET price = CAST(REPLACE(price, 'USD', '') AS DECIMAL(10,2));
    """
    
    return alter_price_sql, update_price_sql

def insert_new_data(source_data):
    """
    Insert new data into the target table
    """
    insert_sql = """
    INSERT INTO pos_data.sales_table (product_id, product_desc, price)
    VALUES (%s, %s, %s);
    """
    
    values = []
    for _, row in source_data.iterrows():
        product_id = row['product_id']
        product_desc = row['product_desc']
        price = transform_price(row['price'])
        values.append((product_id, product_desc, price))
    
    return insert_sql, values

# Main execution
alter_sql, update_sql = update_price_column()

# Execute alter_sql and update_sql here

# Now, insert new data
insert_sql, values = insert_new_data(source_data)

# Execute insert_sql with values here
```

# Output from schema_validator_task (Schema validator)
```python
import pandas as pd
import mysql.connector
from mysql.connector import Error

def clean_price(price):
    """
    Remove 'USD' from price and convert to float
    """
    return float(price.replace(' USD', ''))

def process_sales_data(source_file, target_table):
    # Read source data
    df = pd.read_csv(source_file)
    
    # Clean price column
    df['price'] = df['price'].apply(clean_price)
    
    # Connect to MySQL database
    try:
        connection = mysql.connector.connect(
            host='your_host',
            database='pos_data',
            user='your_username',
            password='your_password'
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Prepare the INSERT statement
            insert_query = f"""
            INSERT INTO {target_table} (product_id, product_desc, price)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
            product_desc = VALUES(product_desc),
            price = VALUES(price)
            """
            
            # Insert or update data
            for _, row in df.iterrows():
                cursor.execute(insert_query, tuple(row))
            
            connection.commit()
            print("Data inserted/updated successfully")
            
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

# Usage
process_sales_data('/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/schema_change/sales.txt', 'sales_table')
```

# Output from schema_validator_task (Schema validator)
```python
import pandas as pd

def process_sales_data(source_df):
    # Convert price column to numeric, removing 'USD'
    source_df['price'] = source_df['price'].str.replace(' USD', '').astype(float)
    
    return source_df

# Read the source CSV file
source_df = pd.read_csv('/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/schema_change/sales.txt')

# Process the data
processed_df = process_sales_data(source_df)

# Connect to the MySQL database
from sqlalchemy import create_engine
engine = create_engine('mysql://username:password@localhost/pos_data')

# Append the processed data to the existing table without dropping historical data
processed_df.to_sql('sales_table', engine, if_exists='append', index=False)
```

# Output from schema_validator_task (Schema validator)
```python
import pandas as pd
import mysql.connector
from mysql.connector import Error

def transform_and_load_data(source_file, target_database, target_table):
    # Read the source CSV file
    df = pd.read_csv(source_file)
    
    # Transform the price column
    df['price'] = df['price'].str.replace(' USD', '').astype(float)
    
    # Connect to the target database
    try:
        connection = mysql.connector.connect(
            host='your_host',
            database=target_database,
            user='your_username',
            password='your_password'
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Prepare the INSERT statement
            insert_query = f"""
            INSERT INTO {target_table} (product_id, product_desc, price)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
            product_desc = VALUES(product_desc),
            price = VALUES(price)
            """
            
            # Insert or update data
            for _, row in df.iterrows():
                cursor.execute(insert_query, tuple(row))
            
            connection.commit()
            print("Data successfully inserted/updated.")
            
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed.")

# Usage
source_file = '/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/schema_change/sales.txt'
target_database = 'pos_data'
target_table = 'sales_table'
transform_and_load_data(source_file, target_database, target_table)
```

# Output from schema_validator_task (Schema validator)
```python
import pandas as pd
import mysql.connector
from mysql.connector import Error

def process_sales_data(source_file, target_table):
    # Read the source CSV file
    df = pd.read_csv(source_file)
    
    # Process the price column
    df['price'] = df['price'].str.replace(' USD', '').astype(float)
    
    # Connect to the MySQL database
    try:
        connection = mysql.connector.connect(
            host='your_host',
            database='pos_data',
            user='your_username',
            password='your_password'
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Prepare the INSERT statement
            insert_query = f"""
            INSERT INTO {target_table} (product_id, product_desc, price)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
            product_desc = VALUES(product_desc),
            price = VALUES(price)
            """
            
            # Insert or update data
            for _, row in df.iterrows():
                cursor.execute(insert_query, tuple(row))
            
            connection.commit()
            print("Data inserted/updated successfully")
            
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

# Usage
source_file = "/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/schema_change/sales.txt"
target_table = "sales_table"
process_sales_data(source_file, target_table)
```

# Output from schema_validator_task (Schema validator)
```python
import pandas as pd
import mysql.connector
from mysql.connector import Error

def process_sales_data(source_file, db_config):
    # Read the source CSV file
    df_source = pd.read_csv(source_file)
    
    # Connect to the MySQL database
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        
        # Alter the 'price' column in the target table to accommodate 'USD'
        alter_price_column_sql = """
        ALTER TABLE sales_table
        MODIFY COLUMN price VARCHAR(20);
        """
        cursor.execute(alter_price_column_sql)
        connection.commit()
        
        # Process and insert data
        for _, row in df_source.iterrows():
            product_id = row['product_id']
            product_desc = row['product_desc']
            price = row['price']
            
            # Check if the product already exists
            check_sql = "SELECT * FROM sales_table WHERE product_id = %s"
            cursor.execute(check_sql, (product_id,))
            existing_product = cursor.fetchone()
            
            if existing_product:
                # Update existing product
                update_sql = """
                UPDATE sales_table
                SET product_desc = %s, price = %s
                WHERE product_id = %s
                """
                cursor.execute(update_sql, (product_desc, price, product_id))
            else:
                # Insert new product
                insert_sql = """
                INSERT INTO sales_table (product_id, product_desc, price)
                VALUES (%s, %s, %s)
                """
                cursor.execute(insert_sql, (product_id, product_desc, price))
            
        connection.commit()
        print("Data processing completed successfully.")
        
    except Error as e:
        print(f"Error: {e}")
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Database configuration
db_config = {
    'host': 'your_host',
    'database': 'pos_data',
    'user': 'your_username',
    'password': 'your_password'
}

# Path to the source CSV file
source_file = '/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/schema_change/sales.txt'

# Process the sales data
process_sales_data(source_file, db_config)
```

# Output from schema_validator_task (Schema validator)
```python
import pandas as pd
import mysql.connector
from mysql.connector import Error

def update_sales_table():
    # SQL to add the new column 'sales_date'
    add_column_sql = """
    ALTER TABLE pos_data.sales_table
    ADD COLUMN sales_date DATE;
    """

    # Python function to update the data
    def update_data():
        try:
            # Establish database connection
            connection = mysql.connector.connect(
                host='your_host',
                database='pos_data',
                user='your_username',
                password='your_password'
            )

            if connection.is_connected():
                cursor = connection.cursor()

                # Execute SQL to add new column
                cursor.execute(add_column_sql)
                connection.commit()

                # Read source data
                df = pd.read_csv('/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/schema_change/sales.txt')

                # Convert price column
                df['price'] = df['price'].str.replace(' USD', '').astype(float)

                # Prepare data for insertion
                data_to_insert = df.to_dict('records')

                # SQL for inserting or updating data
                upsert_sql = """
                INSERT INTO pos_data.sales_table (product_id, product_desc, price, sales_date)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                product_desc = VALUES(product_desc),
                price = VALUES(price),
                sales_date = VALUES(sales_date)
                """

                # Execute upsert for each row
                for row in data_to_insert:
                    cursor.execute(upsert_sql, (
                        row['product_id'],
                        row['product_desc'],
                        row['price'],
                        row['sales_date']
                    ))

                connection.commit()
                print("Data updated successfully")

        except Error as e:
            print(f"Error: {e}")

        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

    # Call the function to update the data
    update_data()

# Call the main function
update_sales_table()
```

# Output from schema_validator_task (Schema validator)
import pandas as pd
import mysql.connector
from mysql.connector import Error
from decimal import Decimal

def transform_and_update_sales_data(source_file, db_config):
    # Read the source CSV file
    df = pd.read_csv(source_file)
    
    # Transform the price column
    df['price'] = df['price'].apply(lambda x: Decimal(x.split()[0]))
    
    # Ensure sales_date is in the correct format
    df['sales_date'] = pd.to_datetime(df['sales_date']).dt.strftime('%Y-%m-%d')
    
    try:
        # Establish database connection
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        # Update existing records and insert new ones
        for _, row in df.iterrows():
            query = """
            INSERT INTO sales_table (product_id, product_desc, price, sales_date)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            product_desc = VALUES(product_desc),
            price = VALUES(price),
            sales_date = VALUES(sales_date)
            """
            cursor.execute(query, (row['product_id'], row['product_desc'], row['price'], row['sales_date']))
        
        conn.commit()
        print(f"{cursor.rowcount} record(s) upserted successfully.")
        
    except Error as e:
        print(f"Error: {e}")
    
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# Example usage:
# db_config = {
#     'host': 'your_host',
#     'database': 'pos_data',
#     'user': 'your_username',
#     'password': 'your_password'
# }
# transform_and_update_sales_data('path_to_your_source_file.csv', db_config)

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

def add_email_column():
    connection = connect_to_database()
    if connection:
        try:
            cursor = connection.cursor()
            add_column_query = """ALTER TABLE raw_pos
                                 ADD COLUMN email VARCHAR(255);"""
            cursor.execute(add_column_query)
            connection.commit()
            print("Email column added successfully.")
        except Error as e:
            print(f"Error adding email column: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

def update_column_types():
    connection = connect_to_database()
    if connection:
        try:
            cursor = connection.cursor()
            alter_queries = [
                "ALTER TABLE raw_pos MODIFY COLUMN product_id VARCHAR(255);",
                "ALTER TABLE raw_pos MODIFY COLUMN alcohol_percentage FLOAT;",
                "ALTER TABLE raw_pos MODIFY COLUMN unit_price DECIMAL(10, 2);",
                "ALTER TABLE raw_pos MODIFY COLUMN total_sales DECIMAL(10, 2);"
            ]
            for query in alter_queries:
                cursor.execute(query)
            connection.commit()
            print("Column types updated successfully.")
        except Error as e:
            print(f"Error updating column types: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

def cast_and_update_data(csv_file_path):
    df = pd.read_csv(csv_file_path)
    
    df['product_id'] = df['product_id'].astype(str)
    df['alcohol_percentage'] = pd.to_numeric(df['alcohol_percentage'], errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['total_sales'] = pd.to_numeric(df['total_sales'], errors='coerce')
    
    connection = connect_to_database()
    if connection:
        try:
            cursor = connection.cursor()
            for _, row in df.iterrows():
                update_query = """UPDATE raw_pos
                                  SET product_id = %s,
                                      alcohol_percentage = %s,
                                      unit_price = %s,
                                      total_sales = %s,
                                      email = %s
                                  WHERE sales_id = %s;"""
                cursor.execute(update_query, (row['product_id'], row['alcohol_percentage'],
                                             row['unit_price'], row['total_sales'],
                                             row['email'], row['sales_id']))
            connection.commit()
            print("Data updated successfully.")
        except Error as e:
            print(f"Error updating data: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

if __name__ == "__main__":
    add_email_column()
    update_column_types()
    cast_and_update_data('/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/combined_testcase/raw_pos.csv')
