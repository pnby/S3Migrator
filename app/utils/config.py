from dotenv import load_dotenv
from envparse import env

load_dotenv()

ACCESS_KEY_ID = env.str("ACCESS_KEY_ID")
SECRET_KEY = env.str("SECRET_KEY")

MYSQL_USER = env.str("MYSQL_USER")
MYSQL_PASSWD = env.str("MYSQL_PASSWD")