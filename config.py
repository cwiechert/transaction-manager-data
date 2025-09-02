import os

from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

# Supabase
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

# Microsoft API
SCOPES = ["Mail.Read"]
GRAPH_API_ENDPOINT = "https://graph.microsoft.com/v1.0/me/messages"
MS_CLIENT_ID = os.getenv('MS_CLIENT_ID')
MS_TENANT_ID = os.getenv('MS_TENANT_ID')
AUTHORITY = f"https://login.microsoftonline.com/{MS_TENANT_ID}"
TM_EMAIL = 'transaction_manager@outlook.com'

# Banco de Chile
SENDER_EMAIL = ['enviodigital@bancochile.cl', 'serviciodetransferencias@bancochile.cl', 'enviodigital@bancoedwards.cl']
TC_SUBJECTS = [ 
    'Giro con Tarjeta de Débito',  
    'Compra con Tarjeta de Crédito',  
    'Cargo en Cuenta',
    'Avance con Tarjeta de Crédito',
    'Cargo en Cuenta'
    ]

