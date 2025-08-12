import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

SUPABASE_USER = os.getenv("SUPABASE_USER")
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")
SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_PORT = os.getenv("SUPABASE_PORT")
SUPABASE_DBNAME = os.getenv("SUPABASE_DBNAME")

db_url = f"postgresql://{SUPABASE_USER}:{SUPABASE_PASSWORD}@{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DBNAME}"
engine = create_engine(db_url)


def get_usd_to_clp_rate():
    """
    Fetches the current USD to CLP exchange rate using Yahoo Finance.
    """
    try:
        # Define the ticker for the USD/CLP pair
        ticker = yf.Ticker("USDCLP=X")

        # Get the data for the last day. The 'Close' price is the most recent rate.
        data = ticker.history(period="1d")

        if not data.empty:
            # Get the last available closing price
            latest_price = data['Close'].iloc[-1]
            return latest_price
        else:
            return "Could not retrieve data. The ticker 'USDCLP=X' may be invalid."

    except Exception as e:
        return f"An error occurred: {e}"


if __name__ == "__main__":
    df = pd.read_sql('transactions', engine)
    rate = get_usd_to_clp_rate()
    df['amount_clp'] = df.apply(
        lambda row: row['amount'] * rate if row['currency'] == 'USD' else row['amount'],
        axis=1
    )
    df.drop(columns=['mail_timestamp_utc']).to_excel('transaction_data.xlsx', index=False)