from youtube_tools.utils.db_requestor import DBRequestor
import os
import sys
import pandas as pd

sys.path.append(os.getcwd())
try:
    import setting
except:
    print("No 'settings.py' file in ", os.getcwd())

import slack
from datetime import datetime
from time import sleep
import json


if __name__ == '__main__':
    a = DBRequestor()
    database = 'youtube'
    username = os.getenv('USERNAME')
    password = os.getenv('PASSWORD')
    host = os.getenv('HOST')
    port = os.getenv('PORT')
    a.get_info_db(database = database, user = username, password = password, host = host, port = port)
    t = datetime.now()
    slackclient = slack.WebClient(token = os.environ['SLACK_TOKEN'])
    slackclient.chat_postMessage(channel = '#data_status',
                                 text = "CHECK IF THE DATA HAS BEEN SAVED TO THE DATABASE OR NOT!")
    crawl_again = False
    with open("/home/thanhnhan/Desktop/khtn_ktxldl/logs/len_data.json", 'r') as hf:
        len_data = json.load(hf)
    try:
        # Check data VN
        data_vn = a.get_df_by_query(
            "select * from vn_ytb_trending where runtime between '{}' and '{}' order by runtime desc;".format(
                t.replace(minute = 0, second = 0, microsecond = 0), t.replace(microsecond = 0)))
        if len(data_vn) == len_data["{}_{}_{}_{}_vn_ytb_trending".format(t.year, t.month, t.day, t.hour)]:
            slackclient.chat_postMessage(channel = '#data_status',
                                         text = "TABLE: vn_ytb_trending! There are {} rows has been saved to the table!".format(
                                             len(data_vn)))
        else:
            slackclient.chat_postMessage(channel = '#data_status',
                                         text = "TABLE: vn_ytb_trending!ERROR! Data will be crawled by contingency API !")
            crawl_again = True

        # Check data World
        data_world = a.get_df_by_query(
            "select * from world_ytb_trending where runtime between '{}' and '{}' order by runtime desc;".format(
                t.replace(minute = 0, second = 0, microsecond = 0), t.replace(microsecond = 0)))

        if len(data_world) == len_data["{}_{}_{}_{}_world_ytb_trending".format(t.year, t.month, t.day, t.hour)]:
            slackclient.chat_postMessage(channel = '#data_status',
                                         text = "TABLE: world_ytb_trending! There are {} rows has been saved to the table!".format(
                                             len(data_world)))
        else:
            slackclient.chat_postMessage(channel = '#data_status',
                                         text = "TABLE: world_ytb_trending!ERROR! Data will be crawled by contingency API !")
            crawl_again = True
    except:
        slackclient.chat_postMessage(channel = '#data_status', text = "ERROR! CAN NOT CHECK DATA!")
        crawl_again = True

    if crawl_again:
        sleep(300)
        os.system("python /home/thanhnhan/Desktop/khtn_ktxldl/run.py -o 10 -a 10")