import os

from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

SUPABASE_USER = os.getenv("SUPABASE_USER")
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")
SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_PORT = os.getenv("SUPABASE_PORT")
SUPABASE_DBNAME = os.getenv("SUPABASE_DBNAME")
DB_URL = (
    f"postgresql://{SUPABASE_USER}:{SUPABASE_PASSWORD}@"
    f"{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DBNAME}"
    )
DB_ENGINE = create_engine(DB_URL)
MS_CLIENT_ID = os.getenv('MS_CLIENT_ID')
MS_TENANT_ID = 'consumers'
AUTHORITY = f"https://login.microsoftonline.com/{MS_TENANT_ID}"
SCOPES = ["Mail.Read"]
GRAPH_API_ENDPOINT = "https://graph.microsoft.com/v1.0/me/messages"
SENDER_EMAIL = ['enviodigital@bancochile.cl', 'serviciodetransferencias@bancochile.cl']
