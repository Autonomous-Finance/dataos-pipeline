from dotenv import load_dotenv


def load_env():
    try:
        load_dotenv("../.env")
    except:
        print('Failed to load .env')
