import pandas as pd
import psycopg2


class DBRequestor():
    def __init__(self):
        self.info_db = None

    def get_info_db(self, **kwargs):
        self.info_db = kwargs

    def get_df_by_query(self, query):
        conn = psycopg2.connect(**self.info_db)
        if conn is None:
            return None
        with conn.cursor() as cur:
            cur.execute(query)
            columns = [desc[0] for desc in cur.description]
            data = cur.fetchall()
            cur.close()
        conn.close()
        if not len(data):
            return pd.DataFrame(data)
        return pd.DataFrame(data, columns=columns)


if __name__ == '__main__':
    a = DBRequestor()
    params = {'database': 'youtube',
              'user': 'youtube',
              'password': '1',
              'host': '27.71.232.95',
              'port': '5432'
              }
    a.get_info_db(**params)
    data = a.get_df_by_query("select * from youtube_trending_all limit 10")
    print(data)
