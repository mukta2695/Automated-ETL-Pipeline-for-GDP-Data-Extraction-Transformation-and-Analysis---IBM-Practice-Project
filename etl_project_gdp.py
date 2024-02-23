import requests
import pandas as pd
import numpy as np
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime
# Code for ETL operations on Country-GDP data

# Importing the required libraries

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ['Country', 'GDP_USD_millions']
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'

def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''

    # Get html page
    html_page = requests.get(url).text
    # Parsing the text into object
    soup = BeautifulSoup(html_page, 'html.parser')
    # Create an empty pandas DataFrame 
    df = pd.DataFrame(columns = table_attribs)
    # Extract all 'tbody' attributes of the HTML object and then extract all the rows of the index 2 table using the 'tr' attribute.
    tables = soup.find_all('tbody')
    rows = tables[2].find_all('tr')

    # Check the contents of each row, having attribute ‘td’, for the following conditions.
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[0].find('a') is not None and '—' not in col[2]:
                # Store all entries matching the conditions in step 5 to a dictionary with keys the same as entries of table_attribs. Append all these dictionaries one by one to the dataframe.

                data_dict = {
                    "Country" : col[0].a.contents[0],
                    "GDP_USD_millions" : col[2].contents[0]                  
                }
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index = True)
        
    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''

    # Convert the contents of the 'GDP_USD_millions' column of df dataframe from currency format to floating numbers.

    GDP_list = df["GDP_USD_millions"].tolist()
    for i in range(len(GDP_list)):
        # Split the string by commas, join them to remove commas, then convert to float
        GDP_list[i] = float("".join(GDP_list[i].split(',')))
        # Divide all these values by 1000 and round it to 2 decimal places.
        GDP_list[i] = np.round(GDP_list[i]/1000, 2)
        # Modify the name of the column from 'GDP_USD_millions' to 'GDP_USD_billions'.
        df = df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"})

        # Assign the transformed GDP_list back to the DataFrame
        df["GDP_USD_billions"] = GDP_list

    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''

    #conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    #conn.close()

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress('Starting ETL process.')

# Extract
df = extract(url, table_attribs)

log_progress('Data extraction complete! Starting Transform!')

# Transform
df = transform(df)

log_progress('Data transformation complete! Starting Load!')

load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')

conn = sqlite3.connect(db_name)
log_progress('SQL Connection initiated.')

load_to_db(df, conn, table_name)

log_progress('Data loaded to Database as table. Running the query')
query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, conn)
log_progress('Process Complete.')
conn.close()

