import re
import json
import logging

from dotenv import load_dotenv
import msal
from bs4 import BeautifulSoup
import requests
import pandas as pd
from sqlalchemy.dialects.postgresql import JSONB

from extract_and_upload_data.config import REASON_MAP, DB_ENGINE
from extract_and_upload_data.config import MS_CLIENT_ID, AUTHORITY, SCOPES
from extract_and_upload_data.config import GRAPH_API_ENDPOINT, SENDER_EMAIL

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Regex for email parsing
MONEY = re.compile(r'(US)?\$([0-9,.]+)')
TC_PAYMENT_MONEY = re.compile(r'(Monto) \$([0-9,.]+)')
TC_TIMESTAMP = re.compile(r'(\d{2}/\d{2}/\d{4}\s\d{2}:\d{2})')
TC_REASON = re.compile(r'\*{4}\d{4} en (.*?) el \d{2}/\d{2}/\d{4}')
CC_PAYMENT_SOURCE = re.compile(r'(\d.*?) Destino')
CC_PAYMENT_DESTINATION = re.compile(r'Destino Nombre y Apellido (.*?) Rut (\d+\-\d)')
CC_TIMESTAMP = re.compile(r'Fecha y Hora:\d+\s()')

# Transactions Map
DEFAULT_CATEGORY = None


def get_access_token(client_id: str, authority: str, scopes: list) -> str:
    """
    Acquires an access token from Microsoft identity platform.
    
    It first tries to get a token from the cache. If that fails, it will
    open a web browser for the user to sign in and grant consent.
    """
    app = msal.PublicClientApplication(
        client_id,
        authority=authority
    )

    accounts = app.get_accounts()
    result = None
    if accounts:
        logging.info("Account found in cache. Attempting to acquire token silently...")
        result = app.acquire_token_silent(scopes, account=accounts[0])

    if not result:
        logging.info("No suitable token in cache. Initiating interactive login.")
        result = app.acquire_token_interactive(
            scopes=scopes,
            prompt="select_account"
        )

    if "access_token" in result:
        return result["access_token"]
    else:
        logging.error("Could not acquire access token.")
        logging.error(result.get("error"))
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


def email_to_dataframe(raw_emails: list) -> pd.DataFrame:
    """
    Converts raw email data to a DataFrame.
    
    Args:
        raw_emails (dict): The raw email data from the Microsoft Graph API.
    Returns:
        pd.DataFrame: A DataFrame containing parsed email data.
    """
    data = []
    for message in raw_emails:
        sender = message['sender']['emailAddress']['address']
        if sender in SENDER_EMAIL:
            try:
                raw_body = message['body']['content']
                soup = BeautifulSoup(raw_body, 'html.parser')
                content = soup.find('body').text
                raw_money = MONEY.findall(content)[0]

                if sender == 'enviodigital@bancochile.cl':
                    transaction_type = 'Pago con TC'
                    transaction_time_local = pd.to_datetime(
                        TC_TIMESTAMP.findall(content)[0], 
                        format='%d/%m/%Y %H:%M'
                        )
                    payment_reason = TC_REASON.findall(content)[0]
                    transferation_type = None
                    transferation_source = None
                    transferation_destination = None

                elif sender == 'serviciodetransferencias@bancochile.cl':
                    transaction_type = 'Transferencia'
                    transaction_time_local = pd.to_datetime(
                        message['sentDateTime'], utc=True
                        ).tz_convert('America/Santiago').tz_localize(None)
                    payment_reason = None
                    transferation_type = message['subject']
                    raw_money = TC_PAYMENT_MONEY.findall(content)[0] if transferation_type in ('Pago de Tarjeta de Crédito Internacional', 'Pago de Tarjeta de Crédito Nacional') else raw_money
                    transferation_source = CC_PAYMENT_SOURCE.findall(content)[0]
                    raw_destination = CC_PAYMENT_DESTINATION.findall(content)
                    if not raw_destination:
                        transferation_destination = None
                    else:
                        transferation_destination = {
                            'nombre': CC_PAYMENT_DESTINATION.findall(content)[0][0],
                            'rut': CC_PAYMENT_DESTINATION.findall(content)[0][1],
                        }
                        
                else:
                    raise ValueError(f"Unexpected sender: {sender}")
                
                row = {
                    'Id': message['id'],
                    'mail_timestamp_utc': pd.to_datetime(message['receivedDateTime']),
                    'transaction_timestamp_local': transaction_time_local,
                    'sender': sender,
                    'currency': 'USD' if raw_money[0] == 'US' else 'CLP',
                    'amount': float(raw_money[1].replace('.', '').replace(',', '.')),
                    'transaction_type': transaction_type,
                    'transferation_type': transferation_type,
                    'transferation_source': transferation_source,
                    'transferation_destination': transferation_destination,
                    'payment_reason': payment_reason,
                    'content': content,
                }
                data.append(row)
            except Exception as e:
                logging.warning(f"Could not parse email {message['id']}: {e}")
                print(raw_money)
                continue
        
    df = pd.DataFrame(data)
    return df


def categorize_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a 'category' column to the DataFrame based on the 'reason' column.

    Args:
        df (pd.DataFrame): The input DataFrame with a 'reason' column.

    Returns:
        pd.DataFrame: The DataFrame with an added 'category' column.
    """
    temp_df = df.copy()
    temp_df.loc[:, 'category'] = temp_df['payment_reason'].apply(
        lambda x: REASON_MAP.get(x, DEFAULT_CATEGORY)
    )
    credit_card_payments = [
        'Pago de Tarjeta de Crédito Internacional',
        'Pago de Tarjeta de Crédito Nacional'
        ]

    temp_df.loc[
        temp_df['transferation_type'].isin(credit_card_payments), 
        'category'
        ] = 'Pago de Tarjeta de Crédito'
    return temp_df


def send_df_to_supabase(df: pd.DataFrame) -> bool:
    """
    Appends a DataFrame to the 'transactions' table in Supabase.

    Args:
        df (pd.DataFrame): The DataFrame to be uploaded.

    Returns:
        bool: True if successful, False otherwise.
    """
    try:
        df.to_sql('transactions', DB_ENGINE, if_exists='append', index=False, dtype={'transferation_destination': JSONB})
        return True
    except Exception as e:
        logging.error(f"Database upload failed: {e}")
        return False


def fetch_supabase_data() -> pd.DataFrame:
    """
    Fetches existing transaction IDs from the Supabase table.
    """
    try:
        return pd.read_sql('SELECT "Id" FROM transactions', DB_ENGINE)
    except Exception as e:
        logging.error(f'Error trying to get data from Supabase: {e}')
        return pd.DataFrame()
    

def update_data(num_emails) -> bool:
    """
    Main function to connect, fetch, categorize, and upload transactions.
    """
    try:
        access_token = get_access_token(MS_CLIENT_ID, AUTHORITY, SCOPES)
    except Exception as e:
        raise ConnectionError(f"MS account connection failed: {e}")

    raw_transactions = get_emails(access_token, num_emails=num_emails)
    transactions_df = email_to_dataframe(raw_transactions) 
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
    categorized_df = categorize_transactions(filtered_transactions)

    if send_df_to_supabase(categorized_df):
        logging.info('Success! Data sent to Supabase.')
    else:
        logging.info('Error sending data to Supabase.')
    
    return True
