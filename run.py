# IMPORT SETTINGS.PY
import os
import sys
from youtube_tools.utils.db_requestor import DBRequestor
from youtube_tools.ytb_trending.get_data import process_data
from sqlalchemy import create_engine
from datetime import datetime
from time import sleep
from youtube_tools.utils.folder_utils import create_folder

sys.path.append(os.getcwd())
try:
    import setting
except:
    print("No 'settings.py' file in ", os.getcwd())

from youtube_tools.utils.logger import setup_logger

import argparse
import slack
import json
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument("-o", "--option", help = "VN or world", default = 0, type = int)  # VN is 1
parser.add_argument("-a", "--api_key", help = "API KEY", default = 10, type = int)  # 1 or 2
args = parser.parse_args()


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


if __name__ == '__main__':
    a = DBRequestor()
    database = 'youtube'
    username = os.getenv('USERNAME')
    password = os.getenv('PASSWORD')
    host = os.getenv('HOST')
    port = os.getenv('PORT')
    a.get_info_db(database = database, user = username, password = password, host = host, port = port)
    df_country_codes = a.get_df_by_query("select * from country_list")
    option = args.option
    t = datetime.now()
    if option == 1:
        log = setup_logger("info_data_youtube_trending",
                           "{}/logs/{}_{}_{}_ytb_trending_VN.txt".format(os.getenv('FOLDER'), t.year,
                                                                         t.month,
                                                                         t.day),
                           mode = 'a+')
        country_codes = ['VN']
        table_name = 'vn_ytb_trending'
    elif option == 0:
        sleep(10)
        log = setup_logger("info_data_youtube_trending",
                           "{}/logs/{}_{}_{}_ytb_trending_all.txt".format(os.getenv('FOLDER'), t.year,
                                                                          t.month,
                                                                          t.day),
                           mode = 'a+')
        country_codes = df_country_codes.country_code
        table_name = 'world_ytb_trending'
    else:
        log = setup_logger("info_data_youtube_trending",
                           "{}/logs/{}_{}_{}_ytb_trending_all_test.txt".format(os.getenv('FOLDER'), t.year,
                                                                               t.month,
                                                                               t.day),
                           mode = 'a+')
        country_codes = ['VN']
        table_name = 'vn_ytb_trending'

    api = args.api_key

    if api == 1:
        api_key = os.getenv('API_KEY_1')
    elif api == 2:
        api_key = os.getenv('API_KEY_2')
    else:
        api_key = os.getenv('API_KEY')

    engine = create_engine('postgresql://{}:{}@{}:5432/{}'.format(username, password, host, database))
    error_folder = "{}/error_folder".format(os.getenv('FOLDER'))
    create_folder(error_folder)
    log_error = setup_logger("error_get_data", "{}/error_get_data.txt".format(error_folder), mode = 'a+')
    slackclient = slack.WebClient(token = os.environ['SLACK_TOKEN'])
    slackclient.chat_postMessage(channel = '#data_status',
                                 text = "TABLE: {}\n START PROCESSING DATA!".format(table_name))
    trending_data = process_data(country_codes, api_key, log, log_error, slackclient)
    t = datetime.now()
    trending_data.to_sql(table_name,
                         con = engine,
                         if_exists = 'append',
                         index = False,
                         method = 'multi'
                         )
    trending_data.to_csv(
        "{}/data/{}{}{}_{}_{}.csv".format(os.getenv('FOLDER'), t.year, t.month, t.day, t.hour, table_name))
    log.info("COMPLETED! SAVED DATA TO THE DATABASE!")
    slackclient.chat_postMessage(channel = '#data_status',
                                 text = "COMPLETED! SAVED DATA TO THE DATABASE!\n{}".format('*' * 50))

    with open("/home/thanhnhan/Desktop/khtn_ktxldl/logs/len_data.json", 'r') as hf:
        info_hf = json.load(hf)

    info_hf["{}_{}_{}_{}_{}".format(t.year, t.month, t.day, t.hour, table_name)] = len(trending_data)
    with open("/home/thanhnhan/Desktop/khtn_ktxldl/logs/len_data.json", 'w') as log_len:
        json.dump(info_hf, log_len, cls = NpEncoder, indent = 4)
