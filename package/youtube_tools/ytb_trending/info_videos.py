import requests
import pandas as pd
import os
import sys

sys.path.append(os.getcwd())
try:
    from youtube_tools import setting
except:
    print("No 'settings.py' file in ", os.getcwd())


def video_categories_mapping():
    # get video category from youtube API
    request_url = f"https://youtube.googleapis.com/youtube/v3/videoCategories?regionCode=VN&key={os.getenv('API_KEY')}"
    request = requests.get(request_url)

    # transform request result to df
    id = []
    video_category = []
    for item in request.json().get('items'):
        id.append(item['id'])
        video_category.append(item['snippet']['title'])

    df_category = pd.DataFrame({'category_id': id, 'video_category': video_category})
    df_category.category_id = df_category.category_id.astype(int)
    return df_category


def get_country_info():
    import pycountry
    # temp = list(pycountry.countries)
    list_country_name = []
    list_country_code = []
    for code in os.getenv('COUNTRY_CODE'):
        list_country_name.append(pycountry.countries.get(alpha_2=code).name)
        list_country_code.append(code)
    country_df = pd.DataFrame()
    country_df['country_name'] = list_country_name
    country_df['country_code'] = list_country_code
    return country_df
