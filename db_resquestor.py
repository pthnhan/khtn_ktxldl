import pandas as pd
import psycopg2

params = {'database': 'youtube',
          'user': 'youtube',
          'password': '1',
          'host': '27.71.232.95',
          'port': '5432'
          }

def get_df_by_query(query):
    conn = psycopg2.connect(**params)
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
    data = get_df_by_query("select * from main limit 10")
    print(data)