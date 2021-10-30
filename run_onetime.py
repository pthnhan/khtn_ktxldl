import requests, time
import pandas as pd
import json

api_key = ''
country_code= ['AL', 'DZ', 'AR', 'AM', 'AU', 'AT', 'AZ', 'BH', 'BD', 'BY', 'BE', 'BO', 'BA', 'BR', 'BG', 'KH', 'CA', 'CL', 'CO', 'CR', 'HR', 'CY', 'CZ', 'DK', 'DO', 'EC', 'EG', 'SV', 'EE', 'FI', 'FR', 'GE', 'DE', 'GH', 'GR', 'GT', 'HN', 'HK', 'HU', 'IS', 'IN', 'ID', 'IQ', 'IE', 'IL', 'IT', 'JM', 'JP', 'JO', 'KZ', 'KE', 'KR', 'KW', 'LA', 'LV', 'LB', 'LY', 'LI', 'LT', 'LU', 'MK', 'MY', 'MT', 'MX', 'MD', 'MN', 'ME', 'MA', 'NP', 'NL', 'NZ', 'NI', 'NG', 'NO', 'OM', 'PK', 'PA', 'PG', 'PY', 'PE', 'PH', 'PL', 'PT', 'PR', 'QA', 'RO', 'RU', 'SA', 'SN', 'RS', 'SG', 'SK', 'SI', 'ZA', 'ES', 'LK', 'SE', 'CH', 'TW', 'TZ', 'TH', 'TN', 'TR', 'UG', 'UA', 'AE', 'GB', 'US', 'UY', 'VE', 'VN', 'YE', 'ZW']
snippet_features = ["title",
                    "publishedAt",
                    "channelId",
                    "channelTitle",
                    "categoryId"]


def video_categories_mapping(api_key):
    # get video category from youtube API
    request_url = f"https://youtube.googleapis.com/youtube/v3/videoCategories?regionCode=VN&key={api_key}"
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

def get_country_table(country_code):
    import pycountry
    # temp = list(pycountry.countries)
    country_name = []
    country_code_ = []
    for code in country_code:
        country_name.append(pycountry.countries.get(alpha_2=code).name)
        country_code_.append(code)
    country_table = pd.DataFrame()
    country_table['country_name'] = country_name
    country_table['country_code'] = country_code_
    return country_table


if __name__ == '__main__':
    from sqlalchemy import create_engine
    df_category = video_categories_mapping(api_key)
    # country_table = get_country_table(country_code)
    hostname = '27.71.232.95'
    username = 'youtube'
    password = '1'
    database = 'youtube'
    engine = create_engine(f'postgresql://{username}:{password}@{hostname}:5432/{database}')
    df_category.to_sql('video_categories',
                         con=engine,
                         if_exists='replace',
                         index=False,
                         method='multi')