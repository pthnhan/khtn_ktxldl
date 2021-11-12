from youtube_tools.utils.db_requestor import DBRequestor
import os
import sys
from sqlalchemy import create_engine

try:
    from youtube_tools import setting
except:
    print("No 'settings.py' file in ", os.getcwd())

a = DBRequestor()
database = 'youtube'
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')
host = os.getenv('HOST')
port = os.getenv('PORT')
a.get_info_db(database=database, user=username, password=password, host=host, port=port)
df = a.get_df_by_query(
    "select * from ytb_trending_world where time_running between '2021-11-01 04:00:00' and '2021-11-01 04:59:00' order by time_running desc;")
df = df.rename(columns={'time_running': 'runtime'})
print(len(df))
engine = create_engine('postgresql://{}:{}@{}:5432/{}'.format(username, password, host, database))
df.to_sql('xyz',
          con=engine,
          if_exists='append',
          index=False,
          method='multi'
          )
