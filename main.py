from outlook import outlook_update
from gmail import gmail_update
from config import get_db_mails

if __name__ == "__main__":
    db_mails = get_db_mails()
    for mail in db_mails['outlook']:
        outlook_update(user_email=mail, num_emails=20)
    for mail in db_mails['gmail']:
        gmail_update(user_email=mail, num_emails=20)
