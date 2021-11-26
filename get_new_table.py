from youtube_tools.utils.db_requestor import DBRequestor
import os
import sys
from tabulate import tabulate
import pandas as pd
from time import sleep
import slack

sys.path.append(os.getcwd())
try:
    import setting
except:
    print("No 'settings.py' file in ", os.getcwd())

from sqlalchemy import create_engine


def get_top10(data):
    data = pd.DataFrame(data,
                        columns = ['runtime', 'video_id', 'title', 'view_count', 'likes', 'dislikes', 'comment_count',
                                   'trending_id', 'category_id'])
    time_list = set(data.runtime.to_list())
    df = pd.DataFrame()
    for t in time_list:
        df_ = data[data.runtime == t]
        df_ = df_.sort_values(by = ['trending_id'], ascending = [True])
        df_ = df_[:10]
        df = pd.concat([df, df_])
    df = df.sort_values(by = ['runtime', 'trending_id'], ascending = [True, True])
    return df


def get_no_of_hours_on_ytb_trending(data_top10, engine, to_sql = False, table_name=None):
    data = pd.DataFrame(data_top10, columns = ['runtime', 'title', 'trending_id'])
    dict_videos = {}
    count_videos = []
    for i in range(len(data)):
        if data.iloc[i].title not in dict_videos:
            dict_videos[data.iloc[i].title] = 1
            count_videos.append(dict_videos[data.iloc[i].title])
        else:
            dict_videos[data.iloc[i].title] += 1
            count_videos.append(dict_videos[data.iloc[i].title])
    data['no_of_hour_on_trending'] = count_videos
    df = data.sort_values(by = ['runtime'], ascending = [True])
    if to_sql:
        df.to_sql(table_name,
                  con = engine,
                  if_exists = 'replace',
                  index = False,
                  method = 'multi',
                  )
    return df


def get_no_of_videos_in_each_category(data, category_data, engine, to_sql = False, table_name=None):
    data = pd.DataFrame(data, columns = ['runtime', 'video_id', 'category_id'])
    data = data.merge(category_data)
    data = data.sort_values(by = ['runtime'], ascending = [True])
    if to_sql:
        data.to_sql(table_name,
                    con = engine,
                    if_exists = 'replace',
                    index = False,
                    method = 'multi',
                    )
    return data


def count_in_each_category(data, category_data, engine, to_sql = False, table_name=None):
    data = pd.DataFrame(data, columns = ['runtime', 'video_id', 'category_id', 'view_count', 'likes', 'dislikes', 'comment_count'])
    data = data.merge(category_data)
    data = data.sort_values(by = ['runtime'], ascending = [True])
    if to_sql:
        data.to_sql(table_name,
                    con = engine,
                    if_exists = 'replace',
                    index = False,
                    method = 'multi',
                    )
    return data


if __name__ == '__main__':
    a = DBRequestor()
    database = 'youtube'
    username = os.getenv('USERNAME')
    password = os.getenv('PASSWORD')
    host = os.getenv('HOST')
    port = os.getenv('PORT')
    a.get_info_db(database = database, user = username, password = password, host = host, port = port)
    engine = create_engine('postgresql://{}:{}@{}:5432/{}'.format(username, password, host, database))
    vn_trending = a.get_df_by_query("select * from vn_ytb_trending")
    category_data = a.get_df_by_query("select * from video_categories")
    data_top10 = get_top10(vn_trending)
    no_of_hours_on_ytb_trending = get_no_of_hours_on_ytb_trending(data_top10, engine, to_sql = True, table_name = "vn_no_of_hours_on_ytb_trending")
    no_of_videos_in_each_category = get_no_of_videos_in_each_category(vn_trending, category_data, engine, to_sql = True, table_name = "vn_no_of_videos_in_each_category")
    no_of_top10videos_in_each_category = get_no_of_videos_in_each_category(data_top10, category_data, engine, to_sql = True, table_name = "vn_no_of_top10videos_in_each_category")
    count_category = count_in_each_category(vn_trending, category_data, engine, to_sql = True, table_name = "vn_count_in_each_category")
    sleep(120)
    slackclient = slack.WebClient(token = os.environ['SLACK_TOKEN'])
    slackclient.chat_postMessage(channel = '#data_status',
                                 text = "CREATED SOME TABLES:\n\t vn_no_of_hours_on_ytb_trending\n\t vn_no_of_videos_in_each_category\n\t vn_no_of_top10videos_in_each_category\n\t vn_count_in_each_category")
