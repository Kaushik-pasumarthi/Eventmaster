import os
from dotenv import load_dotenv
load_dotenv()

PROWESS_API_KEY = os.getenv("PROWESS_API_KEY", "2066115ce12c859b06cc42cfe6458ed4")
SENDBATCH_URL = "https://prowess.cmie.com/api/sendbatch"
GETBATCH_URL = "https://prowess.cmie.com/api/getbatch"
GETREPORT_URL = "https://prowess.cmie.com/api/getreport"

# PostgreSQL Database URL
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://prowess_user:prowess_password@localhost:5432/corporate_actions_db"
)

TMP_DIR = "./tmp"
os.makedirs(TMP_DIR, exist_ok=True)
