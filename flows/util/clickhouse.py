from clickhouse_driver import Client
import os

def get_clickhouse_connection():
    return Client(host=os.environ['CLICKHOUSE_HOST'], port=os.environ['CLICKHOUSE_PORT'],
           user=os.environ['CLICKHOUSE_USER'], password=os.environ['CLICKHOUSE_PASSWORD'],
           database=os.environ['CLICKHOUSE_DB'])