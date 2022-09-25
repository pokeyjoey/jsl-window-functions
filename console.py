import pandas as pd
url = "https://raw.githubusercontent.com/data-eng-10-21/window-functions/main/favorita_transactions.csv"
df = pd.read_csv(url)
df[:2]

import sqlite3
conn = sqlite3.connect('grocery.db')

df.to_sql('store_transactions', conn, index = False, if_exists = 'replace')

print(pd.read_sql('SELECT * FROM store_transactions LIMIT 1', conn))
print()
print()

print('calcualte the average amount of transactions across the entire dataset')
query = """
    SELECT AVG(transactions) as avg_transactions
    FROM store_transactions"""
print(query)
print(pd.read_sql(query, conn))
print()
print()

print('Use window function to calculate the average transactions without reduceing the number of rows')
query = """
    SELECT date, store_nbr, transactions,
    AVG(transactions) OVER() as avg_transactions
    FROM store_transactions
    LIMIT 5"""
print(query)
print(pd.read_sql(query, conn))
print()
print()


print('Use window function to calculate how much a stores transactions deviate from the average')
query = """
    SELECT date, store_nbr, transactions,
        transactions - AVG(transactions) OVER() as diff_from_avg
    FROM store_transactions
    LIMIT 5"""
print(query)
print(pd.read_sql(query, conn))
print()
print()

print('Calculate the average number of transactions for each store')
query = """
    SELECT date, store_nbr, transactions,
        AVG(transactions) OVER (partition by store_nbr) as avg_by_store
    FROM store_transactions
    WHERE store_nbr = 1
    LIMIT 2"""
print(query)
print(pd.read_sql(query, conn))
print()
print()
