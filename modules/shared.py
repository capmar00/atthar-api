from dotenv import dotenv_values
from pyprojroot import here

config = dotenv_values(".env")
DATA_ISTAT_API_PATH = str(here("data/istat_api"))