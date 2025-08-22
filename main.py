from outlook import outlook_update
from gmail import gmail_update

outlook_users = ['c-wiechert@hotmail.com']
google_users = ['tlarrain3@gmail.com']

if __name__ == "__main__":
    for g in google_users:
        gmail_update(user_email=g, num_emails=20)
    for o in outlook_users:
        outlook_update(user_email=o, num_emails=20)
