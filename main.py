import os
import re
from datetime import timezone

from dotenv import load_dotenv
from O365 import Account
import pandas as pd
from sqlalchemy import create_engine

from config import REASON_MAP

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
MS_TENANT_ID = 'consumers'
AUTHORITY = f"https://login.microsoftonline.com/{MS_TENANT_ID}"
SCOPES = ["Mail.Read"]
GRAPH_API_ENDPOINT = "https://graph.microsoft.com/v1.0/me/messages"

# Regex for parsing email
MONEY_RX = re.compile(r'(US)?\$([0-9,.]+)')
TIMESTAMP_RX = re.compile(r'(\d{2}/\d{2}/\d{4}\s\d{2}:\d{2})')
REASON_RX = re.compile(r'\*{4}\d{4} en (.*?) el \d{2}/\d{2}/\d{4}')

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
        print("Account found in cache. Attempting to acquire token silently...")
        result = app.acquire_token_silent(scopes, account=accounts[0])

    if not result:
        print("No suitable token in cache. Initiating interactive login.")
        result = app.acquire_token_interactive(
            scopes=scopes,
            prompt="select_account"
        )

    if "access_token" in result:
        return result["access_token"]
    else:
        print("Could not acquire access token.")
        print(result.get("error"))
        print(result.get("error_description"))
        return None


def get_emails(access_token: str, num_emails: int = 10) -> dict:
    """
    Fetches emails from the Microsoft Graph API.
    
    Args:
        access_token (str): The access token to authenticate the request.
        
    Returns:
        A dictionary containing the JSON response from the API, or None on failure.
    """
    if not access_token:
        print("Cannot get emails without an access token.")
        return None
        
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    # Define query parameters to get the top 10 emails, ordered by received date
    # and selecting only specific fields to be more efficient.
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
        # Raise an exception for bad status codes (4xx or 5xx)
        response.raise_for_status()
        return response.json()['value']
        
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while making the request: {e}")
        return None
    except json.JSONDecodeError:
        print("Failed to decode JSON from response.")
        return None


def email_to_dataframe(raw_emails: dict) -> pd.DataFrame:
    # Regex for parsing email
    MONEY_RX = re.compile(r'(US)?\$([0-9,.]+)')
    TIMESTAMP_RX = re.compile(r'(\d{2}/\d{2}/\d{4}\s\d{2}:\d{2})')
    REASON_RX = re.compile(r'\*{4}\d{4} en (.*?) el \d{2}/\d{2}/\d{4}')
    sender_email = 'enviodigital@bancochile.cl'
    data = []
    for message in raw_emails:
        if message['sender']['emailAddress']['address'] == sender_email:
            try:
                raw_body = message['body']['content']
                soup = BeautifulSoup(raw_body, 'html.parser')
                content = soup.find('body').text
                raw_money = MONEY_RX.findall(content)[0]

                row = {
                    'Id': message['id'],
                    'mail_timestamp_utc': pd.to_datetime(message['receivedDateTime']),
                    'transaction_timestamp_local': pd.to_datetime(
                        TIMESTAMP_RX.findall(content)[0], 
                        format='%d/%m/%Y %H:%M'
                        ),
                    'sender': sender_email,
                    'currency': 'USD' if raw_money[0] == 'US' else 'CLP',
                    'amount': float(raw_money[1].replace('.', '').replace(',', '.')),
                    'reason': REASON_RX.findall(content)[0],
                    'content': content,
                }
                data.append(row)
            except Exception as e:
                print(f"Could not parse email {message['id']}: {e}")
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
    """
    Fetches existing transaction IDs from the Supabase table.
    """
    try:
        return pd.read_sql('SELECT "Id" FROM transactions', DB_ENGINE)
    except Exception as e:
        print(f'Error trying to get data from Supabase: {e}')
        return pd.DataFrame()
    

def main():
    """
    Main function to connect, fetch, categorize, and upload transactions.
    """
    try:
        access_token = get_access_token()
    except Exception as e:
        raise ConnectionError(f"MS account connection failed: {e}")

    transactions_df = get_emails(access_token, num_emails=100)
    if transactions_df.empty:
        print('No transaction emails found to process')
        return

    previous_dataframe = fetch_supabase_data()
    is_new = ~transactions_df['Id'].isin(previous_dataframe['Id'])
    filtered_transactions = transactions_df.loc[is_new]

    if filtered_transactions.empty:
        print("No new transactions found.")
        return

    print(f"Found {len(filtered_transactions)} new transactions to upload.")
    categorized_df = categorize_transactions(filtered_transactions)

    if send_df_to_supabase(categorized_df):
        print('Success! Data sent to Supabase.')
    else:
        print('Error sending data to Supabase.')


if __name__ == '__main__':
    main()