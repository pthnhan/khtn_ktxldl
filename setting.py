# settings.py
from dotenv import load_dotenv, find_dotenv
load_dotenv()

# OR, the same with increased verbosity
load_dotenv(verbose=True, override=True)

if len(find_dotenv()) > 0:
    load_dotenv(find_dotenv())
    print(find_dotenv())