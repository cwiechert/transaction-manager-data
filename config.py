import os
import pandas as pd

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
MS_CLIENT_ID = os.getenv('MS_CLIENT_ID')
MS_TENANT_ID = os.getenv('MS_TENANT_ID')

# Banco de Chile
SENDER_EMAIL = ['enviodigital@bancochile.cl', 'serviciodetransferencias@bancochile.cl', 'enviodigital@bancoedwards.cl']
TC_SUBJECTS = [ 
    'Giro con Tarjeta de Débito',  
    'Compra con Tarjeta de Crédito',  
    'Cargo en Cuenta',
    'Avance con Tarjeta de Crédito'
    ]

# Get DB Mails
def get_db_mails():
    df = pd.read_sql('SELECT email FROM auth.users', DB_ENGINE)
    emails_list = df.email.to_list()
    emails_dict = {
        'outlook': [mail for mail in emails_list if mail.split('@')[1] in ('hotmail.com', 'outlook.com')],
        'gmail': [mail for mail in emails_list if mail.split('@')[1] == 'gmail.com']
        }
    return emails_dict