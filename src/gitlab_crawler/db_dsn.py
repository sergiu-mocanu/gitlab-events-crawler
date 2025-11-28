import os
from dotenv import load_dotenv

load_dotenv()
user = os.getenv('GLCRAWLER_DB_USER')
name = os.getenv('GLCRAWLER_DB_NAME')
pwd  = os.getenv('GLCRAWLER_DB_PASSWORD')

db_dsn = f"postgresql://{user}:{pwd}@localhost:5432/{name}"
