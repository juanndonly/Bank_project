import numpy as np
import pandas as pd
import sqlite3
import requests
import datetime as datetime
from bs4 import BeautifulSoup

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
db_name = 'Banks.db'
table_attribs = ['Name','MC_USD_Billion']
table_name = 'Largest_banks'
csv_path = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'

def log_progress(message):
    log_file_path = './code_log.txt'
    timestamp_format = "%Y-%m-%d %H:%M:%S"
    current_timestamp = datetime.datetime.now().strftime(timestamp_format)
    log_entry = f"{current_timestamp} : {message}\n"
    with open(log_file_path, "a") as log_file:
        log_file.write(log_entry)

def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns = table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows:
        if row.find('td') is not None:
            col = row.find_all('td')
            bank_name = col[1].find_all('a')[1]['title']
            market_cap = col[2].contents[0][:-1]
            data_dict = {"Name": bank_name, "MC_USD_Billion":float(market_cap)}
            df1 = pd.DataFrame(data_dict, index = [0])
            df = pd.concat([df,df1], ignore_index=True)

    return df

def transform(df, csv_path):
    exchange_rate_df = pd.read_csv(csv_path)
    exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']

    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 1) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 3) for x in df['MC_USD_Billion']]

    return df

def load_to_csv(df, output_path):
    df.to_csv('./Largest_banks_data.csv', index=False)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='append', index=False)

def run_query(query_statement, sql_connection):
    cursor = sql_connection.cursor()
    cursor.execute(query_statement)
    result = cursor.fetchall()
    print(f"Query: {query_statement}")
    print(result)

def main():
    log_progress('Preliminaries complete. Initiating ETL process')

    df = extract(url, table_attribs)
    log_progress('Data extraction complete. Initiating Transformation process')

    df = transform(df, csv_path)
    log_progress('Data transformation complete. Initiating loading process')
    print(df)

    load_to_csv(df, csv_path)
    log_progress('Data saved to CSV file')

    sql_connection = sqlite3.connect(db_name)
    log_progress('SQL Connection initiated.')

    load_to_db(df, sql_connection, table_name)
    log_progress('Data loaded to Database as table. Running the queries')

    query_statement_1 = "SELECT * FROM Largest_banks"
    run_query(query_statement_1, sql_connection)

    query_statement_2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
    run_query(query_statement_2, sql_connection)

    query_statement_3 = "SELECT Name FROM Largest_banks LIMIT 5"
    run_query(query_statement_3, sql_connection)

    sql_connection.close()
    log_progress('Server Connection closed')

    log_progress('Process Complete.')

main()
