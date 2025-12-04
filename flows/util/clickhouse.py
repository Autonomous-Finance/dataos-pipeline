from clickhouse_driver import Client
import clickhouse_connect
import os


def get_clickhouse_connection():
    return Client(host=os.environ['CLICKHOUSE_HOST'], port=os.environ['CLICKHOUSE_PORT'],
           user=os.environ['CLICKHOUSE_USER'], password=os.environ['CLICKHOUSE_PASSWORD'],
           database=os.environ['CLICKHOUSE_DB'])


def get_ch_client():
    return clickhouse_connect.get_client(
        host=os.environ['CLICKHOUSE_HOST'],
        port=8123,
        username=os.environ['CLICKHOUSE_USER'],
        password=os.environ['CLICKHOUSE_PASSWORD'],
    )
