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
