import pandas as pd
import requests, time
import warnings
warnings.filterwarnings("ignore")
import os
import sys
from datetime import datetime

sys.path.append(os.getcwd())
try:
    from youtube_tools import setting
except:
    print("No 'settings.py' file in ", os.getcwd())

# List of simple to collect features
snippet_features = ["title",
                    "publishedAt",
                    "channelId",
                    "channelTitle",
                    "categoryId"]

# Used to identify columns, currently hardcoded order
header = ["video_id"] + snippet_features + ["trending_date", "tags", "view_count", "likes", "dislikes",
                                            "comment_count", "thumbnail_link", "comments_disabled",
                                            "ratings_disabled", "description"]


def prepare_feature(feature):
    # Removes any character from the unsafe characters list and surrounds the whole item in quotes
    for ch in ['\n', '"']:
        feature = str(feature).replace(ch, "")
    return f'"{feature}"'


def api_request(page_token, country_code):
    # Builds the URL and requests the JSON from it
    request_url = f"https://www.googleapis.com/youtube/v3/videos?part=id,statistics,snippet{page_token}chart=mostPopular&regionCode={country_code}&maxResults=50&key={os.getenv('API_KEY')}"
    request = requests.get(request_url)
    if request.status_code == 429:
        print("Temp-Banned due to excess requests, please wait and continue later")
        sys.exit()
    return request.json()


def get_tags(tags_list):
    # Takes a list of tags, prepares each tag and joins them into a string by the pipe character
    return prepare_feature("|".join(tags_list))


def get_videos(items):
    lines = []
    for video in items:
        comments_disabled = False
        ratings_disabled = False

        # We can assume something is wrong with the video if it has no statistics, often this means it has been deleted
        # so we can just skip it
        if "statistics" not in video:
            continue

        # A full explanation of all of these features can be found on the GitHub page for this project
        video_id = prepare_feature(video['id'])

        # Snippet and statistics are sub-dicts of video, containing the most useful info
        snippet = video['snippet']
        statistics = video['statistics']

        # This list contains all of the features in snippet that are 1 deep and require no special processing
        features = [prepare_feature(snippet.get(feature, "")) for feature in snippet_features]

        # The following are special case features which require unique processing, or are not within the snippet dict
        description = snippet.get("description", "")
        thumbnail_link = snippet.get("thumbnails", dict()).get("default", dict()).get("url", "")
        trending_date = time.strftime("%Y-%m-%d")
        tags = get_tags(snippet.get("tags", ["[none]"]))
        view_count = statistics.get("viewCount", 0)

        # This may be unclear, essentially the way the API works is that if a video has comments or ratings disabled
        # then it has no feature for it, thus if they don't exist in the statistics dict we know they are disabled
        if 'likeCount' in statistics and 'dislikeCount' in statistics:
            likes = statistics['likeCount']
            dislikes = statistics['dislikeCount']
        else:
            ratings_disabled = True
            likes = 0
            dislikes = 0

        if 'commentCount' in statistics:
            comment_count = statistics['commentCount']
        else:
            comments_disabled = True
            comment_count = 0

        # Compiles all of the various bits of info into one consistently formatted line
        line = [video_id] + features + [prepare_feature(x) for x in [trending_date, tags, view_count, likes, dislikes,
                                                                     comment_count, thumbnail_link, comments_disabled,
                                                                     ratings_disabled, description]]

        # remove "" character in list
        line = [s.replace('"', "") for s in line if (line[1][0] == '"') & (line[1][len(line[1]) - 1] == '"')]
        lines.append(line)

    return lines


def get_pages(country_code, next_page_token="&"):
    country_data = []

    # Because the API uses page tokens (which are literally just the same function of numbers everywhere) it is much
    # more inconvenient to iterate over pages, but that is what is done here.
    while next_page_token is not None:
        # A page of data i.e. a list of videos and all needed data
        video_data_page = api_request(next_page_token, country_code)

        # Get the next page token and build a string which can be injected into the request with it, unless it's None,
        # then let the whole thing be None so that the loop ends after this cycle
        next_page_token = video_data_page.get("nextPageToken", None)
        next_page_token = f"&pageToken={next_page_token}&" if next_page_token is not None else next_page_token

        # Get all of the items as a list and let get_videos return the needed features
        items = video_data_page.get('items', [])
        country_data += get_videos(items)
    return country_data


def process_data(country_codes, log=None):
    df_trending = pd.DataFrame(columns=[header + ["country_code", 'trending_id']])
    count = 1
    log.info("START PROCESSING DATA!")
    for country_code in country_codes:
        print(count, ':', country_code)
        count += 1
        trending = pd.DataFrame(get_pages(country_code), columns=header)
        trending['country_code'] = country_code
        trending['trending_id'] = [i for i in range(1, len(trending)+1)]
        try:
            df_trending = df_trending.append(trending, ignore_index=True, sort=False)
        except:
            df_trending = trending
        if log is not None:
            log.info("country_code: {}, n_row: {}".format(country_code, len(trending)))
    df_trending = df_trending.rename(columns={'publishedAt': 'published_at',
                                              'channelId': 'channel_id',
                                              'channelTitle': 'channel_title',
                                              'categoryId': 'category_id'})
    df_trending.published_at = pd.to_datetime(df_trending.published_at)
    df_trending.trending_date = pd.to_datetime(df_trending.trending_date)
    df_trending[['category_id', 'view_count', 'likes', 'dislikes', 'comment_count']] = df_trending[
        ['category_id', 'view_count', 'likes', 'dislikes', 'comment_count']].astype(int)
    df_trending = df_trending.drop(columns=['comments_disabled', 'ratings_disabled'], axis=0)
    df_trending['time_running'] = datetime.now().replace(microsecond=0)
    if log is not None:
        log.info("WELL DONE!, n_row: {}".format(len(df_trending)))
    return df_trending