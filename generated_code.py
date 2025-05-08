# Output from schema_validator_task (Schema validator)
import pandas as pd
import sqlite3
from typing import List, Dict

def update_and_append_data(source_file: str, target_table: str, db_path: str):
    """
    Update the target table schema if needed and append new data from the source file.

    Args:
    source_file (str): Path to the source CSV file
    target_table (str): Name of the target table in the database
    db_path (str): Path to the SQLite database

    Returns:
    None
    """
    # Read the source CSV file
    df = pd.read_csv(source_file)

    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get the current schema of the target table
    cursor.execute(f"PRAGMA table_info({target_table})")
    target_schema = {row[1]: row[2] for row in cursor.fetchall()}

    # Add missing columns to the target table
    add_missing_columns(cursor, target_table, df.columns, target_schema)

    # Cast product_id to integer
    df['product_id'] = df['product_id'].astype(int)

    # Remove extra columns from the DataFrame
    df = df.drop(columns=['Unnamed: 2'], errors='ignore')

    # Append new data to the target table
    df.to_sql(target_table, conn, if_exists='append', index=False)

    # Commit changes and close the connection
    conn.commit()
    conn.close()

def add_missing_columns(cursor: sqlite3.Cursor, table: str, source_columns: List[str], target_schema: Dict[str, str]):
    """
    Add missing columns to the target table.

    Args:
    cursor (sqlite3.Cursor): SQLite cursor object
    table (str): Name of the target table
    source_columns (List[str]): List of column names in the source file
    target_schema (Dict[str, str]): Current schema of the target table

    Returns:
    None
    """
    for column in source_columns:
        if column not in target_schema and column != 'Unnamed: 2':
            # Determine the appropriate SQLite data type
            if column == 'price':
                data_type = 'REAL'
            else:
                data_type = 'TEXT'

            # Add the new column to the target table
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {data_type}")

# Usage example
source_file = "/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/schema_change/sales.txt"
target_table = "sales_table"
db_path = "path/to/your/database.db"

update_and_append_data(source_file, target_table, db_path)
# This Python code addresses the schema differences and appends the new data to the target table without dropping any existing data. Here's what the code does:
# 1. It defines two functions: `update_and_append_data` and `add_missing_columns`.
# 2. The `update_and_append_data` function:
#    - Reads the source CSV file.
#    - Connects to the SQLite database.
#    - Gets the current schema of the target table.
#    - Adds missing columns to the target table using the `add_missing_columns` function.
#    - Casts the 'product_id' column to integer to match the target schema.
#    - Removes the extra 'Unnamed: 2' column from the DataFrame.
#    - Appends the new data to the target table.
# 3. The `add_missing_columns` function:
#    - Adds any missing columns to the target table.
#    - Determines the appropriate SQLite data type for new columns (REAL for 'price', TEXT for others).
# 4. The code includes a usage example at the end, which you should modify with the correct database path.
# This solution ensures that:
# - The 'price' column is added to the target table if it doesn't exist.
# - The 'product_id' column is cast to integer to match the target schema.
# - The 'Unnamed: 2' column is ignored and not added to the target table.
# - All new data from the source file is appended to the target table without dropping any existing data.


# Output from data_format_check_task (Data Format Change Detector)
# To address the data format changes and ensure that the new data is correctly appended to the existing table without dropping any data, we need to modify the `update_and_append_data` function. Here's the updated code with the necessary changes:
import pandas as pd
import sqlite3
from typing import List, Dict

def fix_data_format_change(df):
    # Convert 'product_id' to string to preserve non-integer values
    df['product_id'] = df['product_id'].astype(str)

    # Format 'price' as string with dollar sign
    df['price'] = df['price'].apply(lambda x: f'${x:.2f}')

    # Add 'date' column with a placeholder value
    df['date'] = pd.NaT

    # Sort the dataframe by 'product_id'
    df = df.sort_values('product_id')

    return df

def update_and_append_data(source_file: str, target_table: str, db_path: str):
    """
    Update the target table schema if needed and append new data from the source file.

    Args:
    source_file (str): Path to the source CSV file
    target_table (str): Name of the target table in the database
    db_path (str): Path to the SQLite database

    Returns:
    None
    """
    # Read the source CSV file
    df = pd.read_csv(source_file)

    # Apply the fix_data_format_change function
    df = fix_data_format_change(df)

    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get the current schema of the target table
    cursor.execute(f"PRAGMA table_info({target_table})")
    target_schema = {row[1]: row[2] for row in cursor.fetchall()}

    # Add missing columns to the target table
    add_missing_columns(cursor, target_table, df.columns, target_schema)

    # Remove extra columns from the DataFrame
    df = df.drop(columns=['Unnamed: 2'], errors='ignore')

    # Append new data to the target table
    df.to_sql(target_table, conn, if_exists='append', index=False)

    # Commit changes and close the connection
    conn.commit()
    conn.close()

def add_missing_columns(cursor: sqlite3.Cursor, table: str, source_columns: List[str], target_schema: Dict[str, str]):
    """
    Add missing columns to the target table.

    Args:
    cursor (sqlite3.Cursor): SQLite cursor object
    table (str): Name of the target table
    source_columns (List[str]): List of column names in the source file
    target_schema (Dict[str, str]): Current schema of the target table

    Returns:
    None
    """
    for column in source_columns:
        if column not in target_schema and column != 'Unnamed: 2':
            # Determine the appropriate SQLite data type
            if column == 'price':
                data_type = 'TEXT'
            elif column == 'date':
                data_type = 'TEXT'
            else:
                data_type = 'TEXT'

            # Add the new column to the target table
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {data_type}")

# Usage example
source_file = "/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/schema_change/sales.txt"
target_table = "sales_table"
db_path = "path/to/your/database.db"

update_and_append_data(source_file, target_table, db_path)
# These changes address the data format differences and ensure that the new data is correctly appended to the existing table:
# 1. We've added the `fix_data_format_change` function to handle the format changes:
#    - It converts 'product_id' to string to preserve non-integer values.
#    - It formats 'price' as a string with a dollar sign.
#    - It adds a 'date' column with placeholder NaT (Not a Time) values.
#    - It sorts the dataframe by 'product_id' to match the target table order.
# 2. In the `update_and_append_data` function, we now call `fix_data_format_change(df)` before processing the data further.
# 3. In the `add_missing_columns` function, we've updated the data type for 'price' to TEXT to match the new format with dollar signs.
# 4. We've also added a condition for the 'date' column, setting its data type to TEXT.
# These modifications ensure that:
# - The 'product_id' column preserves non-integer values.
# - The 'price' column is formatted as a string with a dollar sign.
# - The 'date' column is added to the source data (with placeholder values) and the target table if it doesn't exist.
# - All new data from the source file is appended to the target table without dropping any existing data.
# - The data format is consistent between the source and target, allowing for successful ingestion of new data into the existing table.



# Output from schema_validator_task (Schema validator)
```python
import pandas as pd
import sqlite3

def add_price_column(conn):
    """
    Add the 'price' column to the target table if it doesn't exist.
    """
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(sales_table)")
    columns = [column[1] for column in cursor.fetchall()]
    
    if 'price' not in columns:
        cursor.execute("ALTER TABLE sales_table ADD COLUMN price FLOAT")
        conn.commit()
        print("Added 'price' column to sales_table")

def cast_product_id(df):
    """
    Cast the 'product_id' column to integer type.
    """
    df['product_id'] = pd.to_numeric(df['product_id'], errors='coerce').astype('Int64')
    return df

def append_new_data(source_file, target_table, db_path):
    """
    Append new data from the source file to the target table,
    handling schema differences and type mismatches.
    """
    # Read the source file
    df = pd.read_csv(source_file)
    
    # Cast product_id to integer
    df = cast_product_id(df)
    
    # Connect to the database
    conn = sqlite3.connect(db_path)
    
    # Add the 'price' column to the target table if it doesn't exist
    add_price_column(conn)
    
    # Remove the 'Unnamed: 2' column from the DataFrame if it exists
    if 'Unnamed: 2' in df.columns:
        df = df.drop(columns=['Unnamed: 2'])
    
    # Append the new data to the target table
    df.to_sql(target_table, conn, if_exists='append', index=False)
    
    print(f"Appended {len(df)} rows to {target_table}")
    
    # Close the database connection
    conn.close()

# Usage example
source_file = "/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/schema_change/sales.txt"
target_table = "sales_table"
db_path = "path/to/your/database.db"

append_new_data(source_file, target_table, db_path)
```

This Python code addresses the schema differences and type mismatches between the source file and the target table:

1. The `add_price_column` function adds the 'price' column to the target table if it doesn't exist.
2. The `cast_product_id` function casts the 'product_id' column to an integer type, using pd.to_numeric with 'coerce' to handle any non-numeric values.
3. The `append_new_data` function:
   - Reads the source file
   - Casts the 'product_id' column
   - Adds the 'price' column to the target table
   - Removes the 'Unnamed: 2' column from the DataFrame if it exists
   - Appends the new data to the target table

This solution ensures that:
- The new 'price' column is added to the target table
- The 'product_id' column is properly cast to an integer type
- The 'Unnamed: 2' column is ignored (as it's not in the source data)
- All new data from the source file is appended to the target table without dropping any existing data

To use this code, replace the `db_path` variable with the actual path to your SQLite database file, and then run the `append_new_data` function with the appropriate parameters.

# Output from data_format_check_task (Data Format Change Detector)
To address the data format differences between the new source file and the existing target table, we need to modify the `append_new_data` function in the existing code. Here's the updated version of the function that incorporates the necessary changes:

```python
import pandas as pd
import sqlite3
from datetime import datetime

def fix_data_format_change(df):
    # Convert product_id to string to preserve non-numeric values
    df['product_id'] = df['product_id'].astype(str)
    
    # Format price as currency string
    df['price'] = df['price'].apply(lambda x: f'${x:.2f}')
    
    # Add a date column with the current date
    df['date'] = datetime.now().strftime('%m-%d-%Y')
    
    return df

def add_date_column(conn):
    """
    Add the 'date' column to the target table if it doesn't exist.
    """
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(sales_table)")
    columns = [column[1] for column in cursor.fetchall()]
    
    if 'date' not in columns:
        cursor.execute("ALTER TABLE sales_table ADD COLUMN date TEXT")
        conn.commit()
        print("Added 'date' column to sales_table")

def append_new_data(source_file, target_table, db_path):
    """
    Append new data from the source file to the target table,
    handling schema differences and type mismatches.
    """
    # Read the source file
    df = pd.read_csv(source_file)
    
    # Apply the fix_data_format_change function
    df = fix_data_format_change(df)
    
    # Connect to the database
    conn = sqlite3.connect(db_path)
    
    # Add the 'date' column to the target table if it doesn't exist
    add_date_column(conn)
    
    # Remove the 'Unnamed: 2' column from the DataFrame if it exists
    if 'Unnamed: 2' in df.columns:
        df = df.drop(columns=['Unnamed: 2'])
    
    # Append the new data to the target table
    df.to_sql(target_table, conn, if_exists='append', index=False)
    
    print(f"Appended {len(df)} rows to {target_table}")
    
    # Close the database connection
    conn.close()

# Usage example
source_file = "/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/schema_change/sales.txt"
target_table = "sales_table"
db_path = "path/to/your/database.db"

append_new_data(source_file, target_table, db_path)
```

This updated code addresses the following issues:

1. It keeps the `product_id` as a string to preserve non-numeric values like '3A' and '3B'.
2. It formats the `price` column as a currency string with a dollar sign and two decimal places.
3. It adds a `date` column to the source data with the current date (you may want to adjust this based on your specific requirements).
4. It adds the `date` column to the target table if it doesn't exist.

The `fix_data_format_change` function is now incorporated into the `append_new_data` function, ensuring that the new data is properly formatted before being appended to the target table.

This solution ensures that all new data coming in the file is appended to the table without dropping any existing data, while also addressing the data format differences between the source and target.

# Output from schema_validator_task (Schema validator)
```python
import pandas as pd
import sqlite3

def add_price_column(df):
    """
    Add the 'price' column to the target table if it doesn't exist.
    
    Args:
    df (pandas.DataFrame): The source dataframe containing the 'price' column.
    
    Returns:
    None
    """
    conn = sqlite3.connect('your_database.db')  # Replace with your actual database connection
    cursor = conn.cursor()
    
    # Check if 'price' column exists in the target table
    cursor.execute("PRAGMA table_info(sales_table)")
    columns = [column[1] for column in cursor.fetchall()]
    
    if 'price' not in columns:
        # Add 'price' column to the target table
        cursor.execute("ALTER TABLE sales_table ADD COLUMN price REAL")
        conn.commit()
    
    conn.close()
    print("Added 'price' column to the target table")

def handle_unnamed_column(df):
    """
    Handle the 'Unnamed: 2' column by dropping it from the dataframe.
    
    Args:
    df (pandas.DataFrame): The source dataframe.
    
    Returns:
    pandas.DataFrame: The dataframe with 'Unnamed: 2' column removed.
    """
    if 'Unnamed: 2' in df.columns:
        df = df.drop('Unnamed: 2', axis=1)
    return df

def cast_product_id(df):
    """
    Cast the 'product_id' column to int64 type.
    
    Args:
    df (pandas.DataFrame): The source dataframe.
    
    Returns:
    pandas.DataFrame: The dataframe with 'product_id' cast to int64.
    """
    df['product_id'] = df['product_id'].astype('int64')
    return df

def process_and_append_data(file_path):
    """
    Process the source CSV file and append the data to the target table.
    
    Args:
    file_path (str): Path to the source CSV file.
    
    Returns:
    None
    """
    # Read the source CSV file
    df = pd.read_csv(file_path)
    
    # Apply schema changes
    add_price_column(df)
    df = handle_unnamed_column(df)
    df = cast_product_id(df)
    
    # Connect to the database
    conn = sqlite3.connect('your_database.db')  # Replace with your actual database connection
    
    # Append the processed data to the target table
    df.to_sql('sales_table', conn, if_exists='append', index=False)
    
    conn.close()
    print("Data processed and appended to the target table")

# Usage
file_path = "/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/schema_change/sales.txt"
process_and_append_data(file_path)
```

This Python code addresses the schema differences between the source CSV file and the target table:

1. `add_price_column(df)`: This function adds the 'price' column to the target table if it doesn't exist.
2. `handle_unnamed_column(df)`: This function removes the 'Unnamed: 2' column from the source dataframe, as it's not present in the target schema.
3. `cast_product_id(df)`: This function casts the 'product_id' column to int64 type to match the target schema.
4. `process_and_append_data(file_path)`: This main function reads the source CSV file, applies the necessary schema changes, and appends the processed data to the target table.

To use this code, you need to replace 'your_database.db' with the actual path to your SQLite database file. The code will add the missing 'price' column to the target table, remove the extra 'Unnamed: 2' column from the source data, cast the 'product_id' to the correct type, and append all the new data to the target table without dropping any existing data.

# Output from data_format_check_task (Data Format Change Detector)
To address the data format differences between the source and target dataframes and ensure that new data can be appended to the existing table without dropping any data, we need to modify the `process_and_append_data` function. Here's the updated Python code that includes the `fix_data_format_change` function:

```python
import pandas as pd
import sqlite3

def fix_data_format_change(df):
    # Convert 'product_id' to string to accommodate both numeric and alphanumeric values
    df['product_id'] = df['product_id'].astype(str)
    
    # Format 'price' as currency string
    df['price'] = df['price'].apply(lambda x: f'${x:.2f}')
    
    # Add 'date' column with a default value (you may want to adjust this)
    df['date'] = pd.Timestamp.now().strftime('%d-%m-%Y')
    
    return df

def add_date_column(conn):
    """
    Add the 'date' column to the target table if it doesn't exist.
    
    Args:
    conn (sqlite3.Connection): The database connection.
    
    Returns:
    None
    """
    cursor = conn.cursor()
    
    # Check if 'date' column exists in the target table
    cursor.execute("PRAGMA table_info(sales_table)")
    columns = [column[1] for column in cursor.fetchall()]
    
    if 'date' not in columns:
        # Add 'date' column to the target table
        cursor.execute("ALTER TABLE sales_table ADD COLUMN date TEXT")
        conn.commit()
    
    print("Added 'date' column to the target table")

def process_and_append_data(file_path):
    """
    Process the source CSV file and append the data to the target table.
    
    Args:
    file_path (str): Path to the source CSV file.
    
    Returns:
    None
    """
    # Read the source CSV file
    df = pd.read_csv(file_path)
    
    # Apply schema changes
    df = fix_data_format_change(df)
    
    # Connect to the database
    conn = sqlite3.connect('your_database.db')  # Replace with your actual database connection
    
    # Add 'date' column to the target table if it doesn't exist
    add_date_column(conn)
    
    # Append the processed data to the target table
    df.to_sql('sales_table', conn, if_exists='append', index=False)
    
    conn.close()
    print("Data processed and appended to the target table")

# Usage
file_path = "/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/schema_change/sales.txt"
process_and_append_data(file_path)
```

This updated code addresses the following data format differences:

1. The `fix_data_format_change` function converts the 'product_id' column to string type to accommodate both numeric and alphanumeric values.
2. It formats the 'price' column as a currency string with a dollar sign and two decimal places.
3. It adds a 'date' column with the current date as a default value (you may want to adjust this logic based on your specific requirements).
4. The `add_date_column` function is added to ensure that the 'date' column exists in the target table.

The `process_and_append_data` function now incorporates these changes:

1. It applies the `fix_data_format_change` function to the source dataframe.
2. It calls `add_date_column` to ensure the target table has the 'date' column.
3. It appends the processed data to the target table without dropping any existing data.

To use this code, replace 'your_database.db' with the actual path to your SQLite database file. This solution ensures that the new data coming in from the file will be properly formatted and appended to the existing table without losing any data.

# Output from schema_validator_task (Schema validator)
Based on the schema differences, here are the suggested code changes:

1. SQL code to add the new 'price' column to the target table:

```sql
ALTER TABLE sales_table
ADD COLUMN price DECIMAL(10, 2);
```

2. SQL code to drop the unnecessary 'Unnamed: 2' column from the target table:

```sql
ALTER TABLE sales_table
DROP COLUMN "Unnamed: 2";
```

3. SQL code to alter the 'product_id' column type in the target table:

```sql
ALTER TABLE sales_table
ALTER COLUMN product_id TYPE VARCHAR(255);
```

4. Python function to cast 'product_id' to the correct type:

```python
import pandas as pd

def cast_product_id(df):
    """
    Cast the product_id column to string (object) type.
    
    Args:
        df (pandas.DataFrame): Input DataFrame containing the product_id column.
    
    Returns:
        pandas.DataFrame: DataFrame with product_id column cast to string type.
    """
    df['product_id'] = df['product_id'].astype(str)
    return df

# Example usage:
# df = pd.read_csv('path_to_your_csv_file.csv')
# df = cast_product_id(df)
# Now you can proceed with inserting the data into the target table
```

To implement these changes:

1. Run the SQL statements in your database management system to modify the target table structure.
2. Use the Python function `cast_product_id` when processing the source data before inserting it into the target table.

Make sure to test these changes in a non-production environment before applying them to your production database. Also, ensure that the data in the 'product_id' column can be safely converted to strings without losing any important information.

# Output from data_format_check_task (Data Format Change Detector)
```python
import pandas as pd

def fix_data_format_change(source_df):
    # Convert product_id to string to handle both numeric and alphanumeric values
    source_df['product_id'] = source_df['product_id'].astype(str)
    
    # Format price as string with dollar sign and two decimal places
    source_df['price'] = source_df['price'].apply(lambda x: f'${x:.2f}')
    
    # Add a placeholder date column (you may want to adjust this based on your requirements)
    source_df['date'] = pd.Timestamp.now().strftime('%m-%d-%Y')
    
    # Reorder columns to match target schema
    source_df = source_df[['product_id', 'product_desc', 'price', 'date']]
    
    return source_df

# Example usage:
# source_df = pd.read_csv('/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/schema_change/sales.txt')
# converted_df = fix_data_format_change(source_df)
# Now you can append converted_df to your existing table
```

This Python function `fix_data_format_change` will parse and convert the source data to match the target format for ingesting new data into the existing table. Here's what the function does:

1. Converts 'product_id' to string type to handle both numeric and alphanumeric values in the source data.
2. Formats the 'price' column as a string with a dollar sign prefix and two decimal places to match the target format.
3. Adds a 'date' column with the current date as a placeholder. You may need to adjust this based on your specific requirements for the date information.
4. Reorders the columns to match the target schema.

To use this function:
1. Read your source CSV file into a pandas DataFrame.
2. Pass the DataFrame to the `fix_data_format_change` function.
3. The returned DataFrame will have the correct format to be appended to your existing table.

Note: This function assumes that all new data in the file should be appended to the table without dropping any existing data. If you need to handle duplicates or perform any other data quality checks, you may want to add those steps before appending the data to your table.
