import pandas as pd

def load_sales_data():
    file_path = '/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/schema_change/sales.txt'
    try:
        # Attempt to read the file as a CSV
        df = pd.read_csv(file_path)
        
        # If successful, return the DataFrame
        return df
    except pd.errors.EmptyDataError:
        return 'The file is empty.'
    except pd.errors.ParserError:
        # If CSV parsing fails, try reading as a fixed-width file
        try:
            df = pd.read_fwf(file_path)
            return df
        except Exception as e:
            return f'Error reading the file: {str(e)}'
    except Exception as e:
        return f'Error reading the file: {str(e)}'

# Call the function and print the result
result = load_sales_data()
print(result)
