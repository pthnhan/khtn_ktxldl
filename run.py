# IMPORT SETTINGS.PY
import os
import sys
from youtube_tools.utils.db_requestor import DBRequestor
from youtube_tools.ytb_trending.get_data import process_data
from sqlalchemy import create_engine
from datetime import datetime
sys.path.append(os.getcwd())
try:
    from youtube_tools import setting
except:
    print("No 'settings.py' file in ", os.getcwd())

from youtube_tools.utils.logger import setup_logger

import argparse
parser = argparse.ArgumentParser()
parser.add_argument("-o", "--option", help="VN or world", default=1, type=int)  # VN is 1
args = parser.parse_args()

if __name__ == '__main__':
    a = DBRequestor()
    database = 'youtube'
    username = os.getenv('USERNAME')
    password = os.getenv('PASSWORD')
    host = os.getenv('HOST')
    port = os.getenv('PORT')
    a.get_info_db(database=database, user=username, password=password, host=host, port=port)
    df_country_codes = a.get_df_by_query("select * from country_list")

    if args.option == 1:
        log = setup_logger("info_data_youtube_trending", "/home/pthnhan/Desktop/other/khtn_ktxldl/ytb_trending_VN.txt",
                           mode='a+')
        country_codes = ['VN']
        table_name = 'ytb_trending_vn'
    else:
        log = setup_logger("info_data_youtube_trending", "/home/pthnhan/Desktop/other/khtn_ktxldl/ytb_trending_VN.txt",
                           mode='a+')
        country_codes = df_country_codes.country_code
        table_name = 'ytb_trending_world'

    engine = create_engine('postgresql://{}:{}@{}:5432/{}'.format(username, password, host, database))
    trending_data = process_data(country_codes, log)
    trending_data.to_sql(table_name,
                         con=engine,
                         if_exists='append',
                         index=False,
                         method='multi'
                         )
