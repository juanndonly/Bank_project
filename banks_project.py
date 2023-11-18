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
csv_path = './Largest_banks_data.csv'

def log_progress(message):
    log_file_path = './code_log.txt'
    timestamp_format = "%Y-%m-%d %H:%M:%S"
    current_timestamp = datetime.datetime.now().strftime(timestamp_format)
    log_entry = f"{current_timestamp} : {message}\n"
    with open(log_file_path, "a") as log_file:
        log_file.write(log_entry)

def extract(url, table_attribs):
    page = request.get(url).text
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
    usd_to_eur_rate = exchange_rate_df['USD_to_EUR'].iloc[0]
    usd_to_jpy_rate = exchange_rate_df['USD_to_JPY'].iloc[0]
    usd_to_gbp_rate = exchange_rate_df['USD_to_GBP'].iloc[0]
    df['MC_EUR_Billion'] = df['MC_USD_Billion'] * usd_to_eur_rate
    df['MC_JPY_Billion'] = df['MC_USD_Billion'] * usd_to_jpy_rate
    df['MC_GBP_Billion'] = df['MC_USD_Billion'] * usd_to_gbp_rate

    return df
def load_to_csv(df, output_path):
    df.to_csv(output_path, index=False)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    cursor = sql_connection.cursor()
    cursor.execute(query_statement)
    result = cursor.fetchall()
    print(result)

    query_1 = "SELECT * FROM Largest_banks"
    query_2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
    query_3 = "SELECT Name FROM Largest_banks LIMIT 5"


    run_query(query_1, sql_connection)
    run_query(query_2, sql_connection)
    run_query(query_3, sql_connection)
