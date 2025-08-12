import os
import re
from datetime import timezone

from dotenv import load_dotenv
from O365 import Account
import pandas as pd
from sqlalchemy import create_engine

load_dotenv()

# Credentials
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
MS_TENANT_ID = os.getenv('MS_TENANT_ID')
MS_SECRET = os.getenv('MS_SECRET')
SCOPES = [
    'basic',
    'mailbox',
    'mail_read',
    'mail_send',
    'offline_access'
]

# Regex for parsing email
MONEY_RX = re.compile(r'(US)?\$([0-9,.]+)')
TIMESTAMP_RX = re.compile(r'(\d{2}/\d{2}/\d{4}\s\d{2}:\d{2})')
REASON_RX = re.compile(r'\*{4}\d{4} en (.*?) el \d{2}/\d{2}/\d{4}')

# Transactions Map
DEFAULT_CATEGORY = None
REASON_MAP = {
    'PAYU *UBER TRIP SANTIAGO CHL': 'Transporte',
    'AWTO AWTO CHILE LAS CONDES CL': 'Transporte',
    'AILIME LTDA SANTIAGO CHL': 'Transporte',
    'UBER LAS CONDES CHL': 'Transporte',
    'MERCADOPAGO *CABIFY25 Las Condes CHL': 'Transporte',
    'DLOCAL *AWTO SANTIAGO CL': 'Transporte',
    'LIME*VIAJE ODVY +18885463345 USA': 'Transporte',
    'DLOCAL *UBER RIDES SANTIAGO CHL': 'Transporte',
    'MERCADOPAGO *CABIFYCH Las Condes CHL': 'Transporte',
    'LIME*2 VIAJES ODVY +18885463345 USA': 'Transporte',
    'UBER RIDES UBER RIDES LAS CONDES CHL': 'Transporte',
    'LIME*PAGO ODVY +18885463345 US': 'Transporte',
    'LIME*3 VIAJES ODVY +18885463345 USA': 'Transporte',

    'PAYU *UBER EATS SANTIAGO CHL': 'Comida - Delivery',

    'HELADERIA LAUSINA SANTIAGO CHL': 'Comida - Restaurantes',
    'NIU SUSHI - ELIECER PA SANTIAGO CHL': 'Comida - Restaurantes',
    'EL TOLDO AZUL SANTIAGO CHL': 'Comida - Restaurantes',
    'LA BURGUESIA SANTIAGO CHL': 'Comida - Restaurantes',
    'NIU SUSHI SANTIAGO CHL': 'Comida - Restaurantes',
    'PAN LEON MUT SPA SANTIAGO CHL': 'Comida - Restaurantes',
    'MP *TANTA SPA SANTIAGO CHL': 'Comida - Restaurantes',
    'MC DONALDS COSTANERA C SANTIAGO CHL': 'Comida - Restaurantes',
    'SOCIEDAD GASTRONOMICA DAISANTIAGO CL': 'Comida - Restaurantes',
    'GASTRONOMIA ZHENG CH SANTIAGO CHL': 'Comida - Restaurantes',
    'TUU*bibimpop SANTIAGO CHL': 'Comida - Restaurantes',
    'CARLS JR SANTIAGO CHL': 'Comida - Restaurantes',
    'RESTAURANT OPORTO SANTIAGO CHL': 'Comida - Restaurantes',
    'TANTA/MUU/CANTINA W SANTIAGO CHL': 'Comida - Restaurantes',
    'RIENDA MUT SPA SANTIAGO CHL': 'Comida - Restaurantes',
    'MERPAGO*SICILY OGGI Q VITACURA CHL': 'Comida - Restaurantes',
    'CRUDO SIN CENSURA MUT LAS CONDES CHL': 'Comida - Restaurantes',

    'OKM SUECIA SANTIAGO CHL': 'Comida - Minimarket',
    'OKM CARMEN SYLVA SANTIAGO CHL': 'Comida - Minimarket',
    'EXPRESS TOBALABA SANTIAGO CHL': 'Comida - Minimarket',
    'EXPRESS PLAZA LYON SANTIAGO CHL': 'Comida - Minimarket',
    'OKM SUECIA SANTIAGO CL': 'Comida - Minimarket',
    'ISIDORA SANTIAGO CHL': 'Comida - Minimarket',
    'SUECIA SANTIAGO CHL': 'Comida - Minimarket',
    'SPID MUT - O871 SANTIAGO CHL': 'Comida - Minimarket',
    'SOCIEDAD GASTRONOMICA SANTIAGO CHL': 'Comida - Minimarket',
    'SERVICIOS Y COMERCIAL LAS CONDES CHL ': 'Comida - Minimarket',
    'MERCADOPAGO *MIMARKET Las Condes CHL': 'Comida - Minimarket',
    'SPID PRESIDENTE RIESCO SANTIAGO CHL': 'Comida - Minimarket',
    'VECINAL SANTIAGO CHL': 'Comida - Minimarket',

    'COURSRA*3I5RRV3WAT9VSM MOUNTAIN VIEWUS': 'Educacion',

    'CRUNCHYROLL LAS CONDES CHL': 'Subscripciones',
    'Smart Fit Chile 551138789999 CL': 'Subscripciones',
    'NETFLIX.COM 866-579-7172 USA': 'Subscripciones',

    'CANDELARIA SANTIAGO CHL': 'Entretenimiento',
    'STEAMGAMES.COM 4259522 BELLEVUE USA': 'Entretenimiento',
    'LOS TRONCOS SANTIAGO CHL': 'Entretenimiento',
    'CINEPLANET WEBPAY SANTIAGO CHL': 'Entretenimiento',
    'CINEPLANET COSTANERA SANTIAGO CHL': 'Entretenimiento',
    'Jagex Payment CAMBRIDGE GBR': 'Entretenimiento',

    'JUMBO COSTANERA CENTER SANTIAGO CHL': 'Supermercado',
    'CHINA HOUSE MARKET SANTIAGO CHL': 'Supermercado',
    'UNIMARC SANTA MARIA SANTIAGO CHL': 'Supermercado',
    'PARIS COSTANERA CENTER SANTIAGO CHL': 'Supermercado',

}


def connect_to_ms() -> Account:
    """
    Authenticates with Microsoft Graph API and returns an Account object.

    Returns:
        Account: An authenticated O365 Account object.
    """
    credentials = (MS_CLIENT_ID, MS_SECRET)
    account = Account(credentials, tenant_id=MS_TENANT_ID)
    if not account.is_authenticated:
        # This will open a browser for authentication on the first run
        account.authenticate(
            scopes=SCOPES, redirect_uri='http://localhost:8000/'
        )
    return account


def fetch_emails(ms_account: Account) -> pd.DataFrame:
    """
    Fetches transaction emails from a specified sender and parses them.

    Args:
        ms_account (Account): The authenticated O365 Account object.

    Returns:
        pd.DataFrame: A DataFrame containing parsed transaction data.
    """
    mailbox = ms_account.mailbox()
    inbox = mailbox.inbox_folder()
    sender_email = 'enviodigital@bancochile.cl'
    data = []

    for message in inbox.get_messages(limit=500):
        if message.sender.address == sender_email:
            try:
                body = message.get_body_text()
                raw_money = MONEY_RX.findall(body)[0]

                row = {
                    'Id': message.object_id,
                    'mail_timestamp_utc': message.received.astimezone(timezone.utc),
                    'transaction_timestamp_local': TIMESTAMP_RX.findall(body)[0],
                    'sender': message.sender.address,
                    'currency': 'USD' if raw_money[0] == 'US' else 'CLP',
                    'amount': float(raw_money[1].replace('.', '').replace(',', '.')),
                    'reason': REASON_RX.findall(body)[0],
                    'content': body,
                }
                data.append(row)
            except (IndexError, AttributeError) as e:
                print(f"Could not parse email {message.subject}: {e}")
                continue
    dataframe = pd.DataFrame(data)
    dataframe['transaction_timestamp_local'] = pd.to_datetime(dataframe['transaction_timestamp_local'], format='%d/%m/%Y %H:%M')
    return dataframe


def categorize_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a 'category' column to the DataFrame based on the 'reason' column.

    Args:
        df (pd.DataFrame): The input DataFrame with a 'reason' column.

    Returns:
        pd.DataFrame: The DataFrame with an added 'category' column.
    """
    df['category'] = df['reason'].apply(
        lambda x: REASON_MAP.get(x, DEFAULT_CATEGORY)
    )
    return df


def send_df_to_supabase(df: pd.DataFrame) -> bool:
    """
    Appends a DataFrame to the 'transactions' table in Supabase.

    Args:
        df (pd.DataFrame): The DataFrame to be uploaded.

    Returns:
        bool: True if successful, False otherwise.
    """
    try:
        df.to_sql('transactions', DB_ENGINE, if_exists='append', index=False)
        return True
    except Exception as e:
        print(f"Database upload failed: {e}")
        return False


def fetch_supabase_data() -> pd.DataFrame:
    try:
        return pd.read_sql('SELECT "Id" FROM transactions', DB_ENGINE)
    except Exception as e:
        print(f'Error trying to get data from Supabase: {e}')
        return pd.DataFrame
    

def main():
    """
    Main function to connect, fetch, categorize, and upload transactions.
    """
    try:
        account = connect_to_ms()
    except Exception as e:
        raise ConnectionError(f"MS account connection failed: {e}")

    transactions_df = fetch_emails(account)
    previous_dataframe = fetch_supabase_data()
    filtered_transactions = transactions_df.loc[~transactions_df['Id'].isin(previous_dataframe['Id'])]

    if filtered_transactions.empty:
        print("No new transactions found.")
        return

    categorized_df = categorize_transactions(filtered_transactions)

    if send_df_to_supabase(categorized_df):
        print('Success! Data sent to Supabase.')
    else:
        print('Error sending data to Supabase.')


if __name__ == '__main__':
    main()