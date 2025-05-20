import pandas as pd

# Read the CSV file
df = pd.read_csv('/Users/gaurang/Documents/git/salesanalysisagent/src/salesanalysisagent/data/combined_testcase/pos_raw_sales.csv')

# Display the first few rows of the DataFrame
print(df.head())

# Get basic information about the DataFrame
print(df.info())