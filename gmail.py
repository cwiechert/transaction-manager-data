from datetime import datetime, timezone
import warnings
import logging
import base64
import time
import re
import os

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import BatchHttpRequest
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from bs4 import XMLParsedAsHTMLWarning
from bs4 import BeautifulSoup
import pandas as pd

from config import SENDER_EMAIL, DB_ENGINE, TC_SUBJECTS

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Regex for email parsing
MONEY = re.compile(r'(US)?\$([0-9,.]+)')
TC_PAYMENT_MONEY = re.compile(r'(USD)\s([0-9,.]+)|\$([0-9,.]+)')
TC_TIMESTAMP = re.compile(r'(\d{2}/\d{2}/\d{4}\s\d{2}:\d{2})')
TC_REASON = re.compile(r'\*{4}\d{4} en (.*?) el \d{2}/\d{2}/\d{4}')
CC_PAYMENT_DESTINATION = re.compile(r'Destino Nombre y Apellido (.*?) Rut (\d+\-\d)')
TRANSFER_DEST_T1 = re.compile(r'fondos a ([A-Za-z\s]+)')
TRANSFER_DEST_T2 = re.compile(r"Nombre y Apellido\s+([^\r\n]+)")

# Variables
CREDENTIALS_FILE = 'google_credentials.json'
TOKEN_DIR = 'google_tokens'
GOOGLE_SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']


def get_credentials(email: str) -> Credentials:
    """
    Authenticates with Google API, handling token creation, storage, and refresh.

    Args:
        email (str): The user's email address, used to name the token file.

    Returns:
        Credentials: A valid Google OAuth2 credential object.
    
    Raises:
        Exception: If the authentication flow fails.
    """
    email_name = email.split('@')[0]
    token_file = os.path.join(f'{email_name}_google_token.json')
    
    creds = None
    if os.path.exists(token_file):
        logging.info(f"Loading credentials from token file: {token_file}")
        creds = Credentials.from_authorized_user_file(token_file, GOOGLE_SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logging.info("Credentials have expired. Refreshing token...")
            creds.refresh(Request())
        else:
            logging.info("No valid credentials found. Starting authentication flow...")
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_FILE, GOOGLE_SCOPES
            )
            creds = flow.run_local_server(port=0)
        
        logging.info(f"Saving new credentials to {token_file}")
        with open(token_file, 'w') as token:
            token.write(creds.to_json())
    
    logging.info("Authentication successful.")
    return creds


def get_emails(
        credentials: Credentials, 
        num_emails: int = 50, 
        chunk_size: int = 20
        ) -> list:
    """
    Fetches a specified number of emails from the user's Gmail inbox using batch requests.

    Args:
        credentials (Credentials): Valid Google API credentials.
        num_emails (int): The total number of emails to fetch.
        chunk_size (int): The number of emails to fetch in each batch request.

    Returns:
        list: A list of full email message objects.
    """
    service = build('gmail', 'v1', credentials=credentials)
    
    logging.info(f"Fetching IDs for the latest {num_emails} emails...")
    all_message_ids = []
    page_token = None
    
    while len(all_message_ids) < num_emails:
        remaining = num_emails - len(all_message_ids)
        request_size = min(remaining, 500)  # Gmail API max page size is 500
        
        results = service.users().messages().list(
            userId='me', 
            labelIds=['INBOX'], 
            maxResults=request_size,
            pageToken=page_token
        ).execute()
        
        messages = results.get('messages', [])
        all_message_ids.extend(messages)
        page_token = results.get('nextPageToken')
        if not page_token:
            break

    if not all_message_ids:
        logging.info("No email messages found in the inbox.")
        return []

    logging.info(f"Fetching full content for {len(all_message_ids)} emails in batches of {chunk_size}...")
    full_messages = []

    def batch_callback(request_id, response, exception):
        """Appends successful responses to the list or logs errors."""
        if exception:
            logging.error(f"Error in batch request {request_id}: {exception}")
        else:
            full_messages.append(response)
    
    for i in range(0, len(all_message_ids), chunk_size):
        batch = service.new_batch_http_request(callback=batch_callback)
        chunk = all_message_ids[i:i + chunk_size]
        for message in chunk:
            batch.add(service.users().messages().get(userId='me', id=message['id'], format='full'))
        
        batch.execute()
        time.sleep(1)

    logging.info(f"Successfully fetched {len(full_messages)} emails.")
    return full_messages


def _get_body_data(part: dict) -> str | None:
    """
    Recursively searches an email's payload parts for the body data (HTML or plain text).

    Args:
        part (dict): A part of the email payload.

    Returns:
        str | None: The base64-encoded email body data, or None if not found.
    """
    # Prefer HTML over plain text
    if part.get("mimeType") == "text/html" and "data" in part.get("body", {}):
        return part["body"]["data"]
    
    # Fallback to plain text
    if part.get("mimeType") == "text/plain" and "data" in part.get("body", {}):
        return part["body"]["data"]
    
    # If the part has sub-parts, search recursively
    if "parts" in part:
        for sub_part in part["parts"]:
            body_data = _get_body_data(sub_part)
            if body_data:
                return body_data
                
    return None


def get_body_text(message: dict) -> str:
    """Placeholder for your email body extraction logic."""
    data = _get_body_data(message['payload'])
    soup = BeautifulSoup(base64.urlsafe_b64decode(data).decode('utf-8'), 'lxml')
    return soup.text


def _parse_amount(raw_money_str: str) -> float:
    """Cleans and converts a string amount to a float."""
    return float(raw_money_str.replace('.', '').replace(',', '.'))


def parse_credit_card_purchase(subject: str, content: str, headers: dict) -> dict | None:
    """Parses a credit card purchase email."""
    if subject not in TC_SUBJECTS:
        return None
        
    try:
        raw_money_match = MONEY.findall(content)[0]
        timestamp_str = TC_TIMESTAMP.findall(content)[0]
        reason = TC_REASON.findall(content)[0]
    except IndexError:
        # If essential info is missing, we can't parse it.
        return None

    return {
        'transaction_timestamp_local': pd.to_datetime(timestamp_str, format='%d/%m/%Y %H:%M'),
        'currency': 'USD' if raw_money_match[0] == 'US' else 'CLP',
        'amount': _parse_amount(raw_money_match[1]),
        'transaction_type': subject,
        'payment_reason': reason,
        'transferation_type': None,
        'transferation_destination': None
    }

def parse_fund_transfer(subject: str, content: str, headers: dict) -> dict | None:
    """Parses a fund transfer email."""
    if 'transferencia' not in subject.lower() or 'Transferencias de Fondos de' in subject:
        return None

    try:
        raw_money_match = MONEY.findall(content)[0]
    except IndexError:
        return None
        
    destination = None
    if subject.startswith('Transferencias de Fondos a'):
        dest_match = TRANSFER_DEST_T1.findall(content)
        if dest_match: destination = dest_match[0]
    elif subject == 'Transferencia a Terceros':
        dest_match = TRANSFER_DEST_T2.findall(content)
        if dest_match: destination = dest_match[0]

    return {
        'transaction_timestamp_local': pd.to_datetime(headers['Date'], utc=True).tz_localize(None),
        'currency': 'USD' if raw_money_match[0] == 'US' else 'CLP',
        'amount': _parse_amount(raw_money_match[1]),
        'transaction_type': 'Transferencia',
        'payment_reason': None,
        'transferation_type': 'Transferencia a Terceros',
        'transferation_destination': destination
    }
    
def parse_card_payment(subject: str, content: str, headers: dict) -> dict | None:
    """Parses a credit card payment confirmation email."""
    is_international = (subject == 'Comprobante Pago Tarjeta Internacional')
    is_national = (subject == 'Comprobante Pago Tarjeta')

    if not is_international and not is_national:
        return None

    try:
        money_matches = TC_PAYMENT_MONEY.findall(content)[0]
        # For international, amount is at index 1 (USD). For national, index 2 (CLP).
        amount_str = money_matches[1] if is_international else money_matches[2]
    except IndexError:
        return None

    return {
        'transaction_timestamp_local': pd.to_datetime(headers['Date'], utc=True).tz_localize(None),
        'currency': 'USD' if is_international else 'CLP',
        'amount': _parse_amount(amount_str),
        'transaction_type': 'Pago de Tarjeta de Crédito',
        'payment_reason': None,
        'transferation_type': subject,
        'transferation_destination': None
    }


def parse_sender(header_dict: dict) -> str | None:
    """Extract sender email address from headers."""
    raw_sender = header_dict.get('From', '')
    email_regex = r'[\w\.-]+@[\w\.-]+\.\w{2,}'
    match = re.search(email_regex, raw_sender)
    if not match:
        logging.warning(f"Could not parse sender from: {raw_sender}")
        return None
    return match.group(0)


def clean_amount(raw_money: str) -> float:
    """Normalize money string into float."""
    return float(raw_money.replace('.', '').replace(',', '.'))


def parse_transaction_time(date_str: str) -> datetime:
    """Convert header 'Date' to naive datetime in local timezone."""
    return pd.to_datetime(date_str, utc=True).tz_localize(None)


def parse_transfer_destination(subject: str, content: str) -> str | None:
    """Extract transfer destination depending on subject type."""
    if subject.startswith("Transferencias de Fondos a"):
        return TRANSFER_DEST_T1.findall(content)[0]
    elif subject == "Transferencia a Terceros":
        return TRANSFER_DEST_T2.findall(content)[0]
    return None


def email_to_dataframe(raw_emails: list) -> pd.DataFrame:
    """
    Processes a list of raw email messages into a structured pandas DataFrame.
    """
    rows = []

    for message in raw_emails:
        headers = message['payload']['headers']
        header_dict = {h['name']: h['value'] for h in headers}

        sender = parse_sender(header_dict)
        subject = header_dict.get('Subject', '')

        if (
            sender not in SENDER_EMAIL
            or subject == 'Aviso de transferencia de fondos'
            or 'Transferencias de Fondos de' in subject
        ):
            logging.debug(f"Skipping email {message['id']} with subject: {subject}")
            continue

        content = get_body_text(message)
        transaction_type = None
        payment_reason = None
        transfer_type = None  
        transfer_destination = None

        # --- Case: TC subjects ---
        if subject in TC_SUBJECTS:
            raw_money = MONEY.findall(content)[0]
            currency = 'USD' if raw_money[0] == 'US' else 'CLP'
            raw_money = raw_money[1]

            transaction_type = subject
            transaction_time_local = pd.to_datetime(
                TC_TIMESTAMP.findall(content)[0],
                format='%d/%m/%Y %H:%M'
            )
            try:
                payment_reason = TC_REASON.findall(content)[0]
            except IndexError:
                payment_reason = subject

        # --- Case: Transferencias ---
        elif ('transferencia' in subject.lower() 
              and 'Transferencias de Fondos de' not in subject.lower()):
            raw_money = MONEY.findall(content)[0]
            currency = 'USD' if raw_money[0] == 'US' else 'CLP'
            raw_money = raw_money[1]

            transaction_type = 'Transferencia'
            transaction_time_local = parse_transaction_time(header_dict['Date'])
            transfer_type = 'Transferencia a Terceros'
            transfer_destination = parse_transfer_destination(subject, content)

        # --- Case: Pago Tarjeta ---
        elif subject == 'Comprobante Pago Tarjeta':
            raw_money = TC_PAYMENT_MONEY.findall(content)[0][2]
            currency = 'USD' if raw_money[0] == 'US' else 'CLP'

            transaction_type = 'Pago de Tarjeta de Crédito'
            transaction_time_local = parse_transaction_time(header_dict['Date'])
            transfer_type = subject

        # --- Case: Pago Tarjeta Internacional ---
        elif subject == 'Comprobante Pago Tarjeta Internacional':
            raw_money = TC_PAYMENT_MONEY.findall(content)[0][1]
            currency = 'USD'

            transaction_type = 'Pago de Tarjeta de Crédito'
            transaction_time_local = parse_transaction_time(header_dict['Date'])
            transfer_type = subject

        else:
            logging.debug(f"Skipping unhandled subject:\nSender:{sender}\nSubject{subject}")
            continue

        row = {
            'Id': message['id'],
            'mail_timestamp_utc': datetime.fromtimestamp(
                int(message['internalDate']) / 1000, tz=timezone.utc
            ),
            'transaction_timestamp_local': transaction_time_local,
            'sender': sender,
            'currency': currency,
            'amount': clean_amount(raw_money),
            'transaction_type': transaction_type,
            'transferation_type': transfer_type,  # kept original field name
            'transferation_destination': transfer_destination,
            'payment_reason': payment_reason,
            'content': content.strip(),
            'user_email': header_dict.get('To'),
        }
        rows.append(row)
    
    df = pd.DataFrame(rows)
    if not df.empty:
        df['content'] = df['content'].str.replace(r'\s+', ' ', regex=True)
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
        logging.warning(e)
        return False


def fetch_supabase_data(user_email: str) -> pd.DataFrame:
    """
    Fetches existing transaction IDs from the Supabase table.
    """
    try:
        return pd.read_sql(f"SELECT * FROM transactions WHERE user_email = '{user_email}'", DB_ENGINE)
    except Exception as e:
        return pd.DataFrame()
    

def gmail_update(user_email: str, num_emails: int) -> bool:
    """
    The main function to orchestrate the entire data pipeline.

    1. Gets Google credentials.
    2. Fetches emails.
    3. Parses them into a DataFrame.
    4. Filters out already existing transactions.
    5. Uploads new transactions to the database.

    Args:
        user_email (str): The email account to process.
        num_emails (int): The number of recent emails to scan.

    Returns:
        bool: True if the process completed successfully, False otherwise.
    """
    try:
        cred = get_credentials(user_email)
    except Exception as e:
        logging.critical(f"Google account connection failed: {e}")
        return False
    
    raw_transactions = get_emails(credentials=cred, num_emails=num_emails)

    transactions_df = email_to_dataframe(raw_emails=raw_transactions)
    if transactions_df.empty:
        logging.info('No relevant transaction emails found to process.')
        logging.info(f"Finished processing User: {user_email}\n")
        return

    previous_dataframe = fetch_supabase_data(user_email=user_email)
    is_new = ~transactions_df['Id'].isin(previous_dataframe['Id'])
    filtered_transactions = transactions_df.loc[is_new]

    if filtered_transactions.empty:
        logging.info("No new transactions found after filtering against the database.")
        logging.info(f"Finished processing User: {user_email}\n")
        return

    logging.info(f"Found {len(filtered_transactions)} new transactions to upload.")

    if send_df_to_supabase(df=filtered_transactions):
        logging.info('Success! Data pipeline completed and new data was uploaded.')
        logging.info(f"Finished processing User: {user_email}\n")
        return True
    else:
        logging.error('Error! The data pipeline failed during the upload step.')
        logging.info(f"Finished processing User: {user_email}\n")
        return False
