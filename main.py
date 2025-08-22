from outlook import update_data as outlook_update
from gmail import google_update_data

users = ['c-wiechert@hotmail.com', 'tlarrain3@hotmail.com']

if __name__ == "__main__":
    google_update_data(user_email='tlarrain3@hotmail.com', num_emails=2000)
