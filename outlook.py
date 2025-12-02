import re
import json
import logging
import os
import atexit

from dotenv import load_dotenv
import msal
from bs4 import BeautifulSoup
import requests
import pandas as pd

from config import DB_ENGINE, MS_CLIENT_ID, AUTHORITY, SCOPES, GRAPH_API_ENDPOINT
from config import SENDER_EMAIL, TC_SUBJECTS, TM_EMAIL

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Regex for email parsing
SENDER_RX = re.compile(r'From:\s*([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)|\<(.*?)\>')
MONEY = re.compile(r'(US)?\$([0-9,.]+)')
TC_PAYMENT_MONEY = re.compile(r'(Monto) \$([0-9,.]+)')
TC_TIMESTAMP = re.compile(r'(\d{2}/\d{2}/\d{4}\s\d{2}:\d{2})')
TC_REASON = re.compile(r'\*{4}\d{4} en (.*?) el \d{2}/\d{2}/\d{4}')
CC_PAYMENT_DESTINATION = re.compile(r'Nombre( y Apellido)?(.*?)\s?Rut')


def get_access_token(client_id: str, authority: str, scopes: list, username: str) -> str:
    """
    Acquires an access token from Microsoft identity platform.
    Uses a persistent file cache to avoid repeated logins.
    If `username` is cached, uses it silently.
    Otherwise, triggers interactive login.

    Args:
        client_id (str): The client ID of the Azure AD application.
        authority (str): The authority URL for the Microsoft identity platform.
        scopes (list): The scopes for which the token is requested.
        username (str): User mail inbox that we're getting.
    Returns:
        str: The access token if successful, None otherwise.
    """
    cache_file = "msal_cache.bin"
    cache = msal.SerializableTokenCache()

    if os.path.exists(cache_file):
        with open(cache_file, "r") as f:
            cache.deserialize(f.read())

    def save_cache():
        if cache.has_state_changed:
            with open(cache_file, "w") as f:
                f.write(cache.serialize())

    atexit.register(save_cache)

    app = msal.PublicClientApplication(
        client_id,
        authority=authority,
        token_cache=cache
    )

    accounts = app.get_accounts(username=username)
    result = None

    if accounts:
        logging.info(f"Found cached account for {username}. Attempting silent token acquisition.")
        result = app.acquire_token_silent(scopes, account=accounts[0])

    if not result:
        logging.info(f"No cached token for {username}. Initiating interactive login.")
        result = app.acquire_token_interactive(
            scopes=scopes,
            login_hint=username,
            prompt="select_account"
        )

    if result and "access_token" in result:
        logging.info(f"Access token acquired for {username}.")
        return result["access_token"]

    logging.error(f"Token acquisition failed: {result.get('error')} - {result.get('error_description')}")
    return None


def get_emails(access_token: str, num_emails: int = 50) -> dict:
    """
    Fetches emails from the Microsoft Graph API.
    
    Args:
        access_token (str): The access token to authenticate the request.
        num_emails (int): The number of emails to fetch.
        
    Returns:
        A dictionary containing the JSON response from the API, or None on failure.
    """
    if not access_token:
        logging.info("Cannot get emails without an access token.")
        return None
        
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    query_params = {
        '$top': num_emails,
        '$orderby': 'receivedDateTime DESC'
    }

    try:
        response = requests.get(
            GRAPH_API_ENDPOINT,
            headers=headers,
            params=query_params
        )
        response.raise_for_status()
        return response.json()['value']
        
    except requests.exceptions.RequestException as e:
        logging.info(f"An error occurred while making the request: {e}")
        return None
    except json.JSONDecodeError:
        logging.info("Failed to decode JSON from response.")
        return None


def get_auth_users() -> pd.DataFrame:
    """
    Fetches existing authenticated user_emails from the Supabase table.
    """
    try:
        return pd.read_sql(f"SELECT email FROM auth.users", DB_ENGINE)
    except Exception as e:
        return pd.DataFrame()


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
        logging.error(f"Database upload failed: {e}")
        return False


def fetch_supabase_data() -> pd.DataFrame:
    """
    Fetches existing transaction IDs from the Supabase table.
    """
    try:
        return pd.read_sql(f'SELECT "Id" FROM transactions', DB_ENGINE)
    except Exception as e:
        return pd.DataFrame()
    

def email_to_dataframe(raw_emails: list) -> pd.DataFrame:
    """
    Converts raw email data to a DataFrame.
    
    Args:
        raw_emails (dict): The raw email data from the Microsoft Graph API.
    Returns:
        pd.DataFrame: A DataFrame containing parsed email data.
    """
    data = []
    auth_users = get_auth_users()['email'].to_list()
    for message in raw_emails:
        forwarder = message['sender']['emailAddress']['address']
        if forwarder not in auth_users:
            continue # Skip emails not forwarded by authenticated users

        try:
            raw_body = message['body']['content']
            soup = BeautifulSoup(raw_body, 'html.parser')
            content = soup.find('body').text
            raw_sender = SENDER_RX.findall(content)[0]
            sender = raw_sender[1] if raw_sender[0] == '' else raw_sender[0]

        except KeyError:
            logging.warning('Empty email detected, continueing with the next one...')
            continue

        if sender in SENDER_EMAIL:
            try:
                subject = message['subject'][4:] # Take out the "Fw: " and "FW: " in front of the subject
                transaction_type = None
                payment_reason = None
                transferation_type = None  
                transferation_destination = None
                
                # Pagos y Avances con Tarjeta Credito
                if subject in TC_SUBJECTS:
                    raw_money = MONEY.findall(content)[0]
                    currency = 'USD' if raw_money[0] == 'US' else 'CLP'
                    amount = float(raw_money[1].replace('.', '').replace(',', '.'))

                    transaction_type = subject
                    transaction_time_local = pd.to_datetime(
                        TC_TIMESTAMP.findall(content)[0], 
                        format='%d/%m/%Y %H:%M'
                        )
                    try:
                        payment_reason = TC_REASON.findall(content)[0]
                    except IndexError:
                        payment_reason = subject

                # Transferencias
                elif ('transferencia' in subject.lower()):
                    raw_money = MONEY.findall(content)[0]
                    currency = 'USD' if raw_money[0] == 'US' else 'CLP'
                    amount = float(raw_money[1].replace('.', '').replace(',', '.'))

                    transaction_type = 'Transferencia'
                    transaction_time_local = pd.to_datetime(
                        message['sentDateTime'], utc=True
                        ).tz_convert('America/Santiago').tz_localize(None)
                    transferation_type = 'Transferencia a Terceros'
                    
                    raw_destination = CC_PAYMENT_DESTINATION.findall(content)

                    if raw_destination:
                        transferation_destination = raw_destination[0][1]
                
                # Pago saldo Tarjeta de Credito
                elif subject in ['Pago de Tarjeta de Crédito Nacional', 'Pago de Tarjeta de Crédito Internacional']:
                    raw_money = TC_PAYMENT_MONEY.findall(content)[0]
                    currency = 'CLP'
                    amount = raw_money[1]

                    transaction_type = 'Pago de Tarjeta de Crédito'
                    transaction_time_local = pd.to_datetime(
                        message['sentDateTime'], utc=True
                        ).tz_convert('America/Santiago').tz_localize(None)
                    transferation_type = subject
                        
                else:
                    logging.debug(f"Skipping unhandled subject:\nSender:{sender}\nSubject{subject}")
                    continue
                
                row = {
                    'Id': message['id'],
                    'mail_timestamp_utc': pd.to_datetime(message['receivedDateTime']),
                    'transaction_timestamp_local': transaction_time_local,
                    'sender': sender,
                    'currency': currency,
                    'amount': amount,
                    'transaction_type': transaction_type,
                    'transferation_type': transferation_type,
                    'transferation_destination': transferation_destination,
                    'payment_reason': payment_reason,
                    'content': content,
                    'user_email': message['from']['emailAddress']['address'] 
                }
                data.append(row)
                
            except Exception as e:
                logging.warning(f"message_id: {message['id']}\nError: {e}")
                continue
        
    df = pd.DataFrame(data)

    return df
    

def outlook_update(num_emails: int) -> bool:
    """
    Main function to connect, fetch, categorize, and upload transactions.
    """
    try:
        access_token = get_access_token(
            client_id=MS_CLIENT_ID, 
            authority=AUTHORITY, 
            scopes=SCOPES,
            username=TM_EMAIL
            )
    except Exception as e:
        raise ConnectionError(f"MS account connection failed: {e}")

    raw_transactions = get_emails(access_token=access_token, num_emails=num_emails)
    transactions_df = email_to_dataframe(raw_emails=raw_transactions) 
    if transactions_df.empty:
        logging.info('No transaction emails found to process')
        return

    previous_dataframe = fetch_supabase_data()
    is_new = ~transactions_df['Id'].isin(previous_dataframe['Id'])
    filtered_transactions = transactions_df.loc[is_new]

    if filtered_transactions.empty:
        logging.info("No new transactions found.")
        return

    logging.info(f"Found {len(filtered_transactions)} new transactions to upload.")

    if send_df_to_supabase(df=filtered_transactions):
        logging.info('Success! Data sent to Supabase.')
        return True
    else:
        logging.info('Error sending data to Supabase.')
        return False
    