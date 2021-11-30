from youtube_tools.utils.db_requestor import DBRequestor
import os
import sys
from sqlalchemy import create_engine
from youtube_tools.ytb_trending.info_videos import video_categories_mapping, get_country_info

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
df1 = video_categories_mapping()
df2 = get_country_info()
print(len(df1))
engine = create_engine('postgresql://{}:{}@{}:5432/{}'.format(username, password, host, database))
df1.to_sql('video_categories',
           con = engine,
           if_exists = 'replace',
           index = False,
           method = 'multi'
           )
df2.to_sql('country_list',
           con = engine,
           if_exists = 'replace',
           index = False,
           method = 'multi'
           )
