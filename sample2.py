import os
import pandas as pd
from sqlalchemy import create_engine


table_name = 'table_x'

if not os.path.exists('csv_database.db'):
    print("Database doesn't exist!")


if os.path.exists('csv_database.db'):
    print('Database exists!')
    csv_database = create_engine('sqlite:///csv_database.db')

    df3 = pd.read_sql_query('SELECT count(distinct MemberReference) FROM table_x;', csv_database)
    print(df3)

