import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

GOOGLE_EMAIL_ADDRESS = os.getenv("GOOGLE_EMAIL_ADDRESS")
RESERVANTO_USERNAME = os.getenv("RESERVANTO_USERNAME")
RESERVANTO_PASSWORD = os.getenv("RESERVANTO_PASSWORD")
CREDENTIALS_PATH = os.getenv("CREDENTIALS_PATH", "../credentials.json")

RESERVANTO_DIR = Path(__file__).parent.parent  # \your\home\directory\reservanto\
