import os
import pandas as pd
from sqlalchemy import create_engine

csv_database = None
table_name = 'table_x'
filename = 'pr_file.csv'
database_name = 'csv_database' + filename.replace('.csv', '') + '.db'

if not os.path.exists('csv_database.db'):
    print('*****************************************************')
    print('Creating engine and reading data from csv file')
    csv_database = create_engine('sqlite:///csv_database.db')
    chunksize = 100000
    i = 0
    j = 1
    for df in pd.read_csv(filename, chunksize=chunksize, iterator=True):
        print('Appending Chunk {0}'.format(i+1))
        df = df.rename(columns={c: c.replace(' ', '') for c in df.columns})
        df.index += j
        i += 1
        df.to_sql(table_name, csv_database, if_exists='append')
        j = df.index[-1] + 1
    print('Database created!!')


if csv_database is not None:
    print('*****************************************************')
    print('Executing query!')
    q1 = """SELECT count(*) FROM table_x;"""
    print(q1, '\n')
    df3 = pd.read_sql_query(q1, csv_database)
    # print(df3)
    # print(df3.values.tolist())
    count = 0
    for idx, row in df3.iterrows():
        if count < 10:
            print(idx, ' : ', row)
            count += 1
    print('*****************************************************')
