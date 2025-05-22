import smtplib
import os
from email.message import EmailMessage

def main():
    smtp_server = 'smtp.gmail.com'
    smtp_port = 587
    username = os.environ['SMTP_USERNAME']
    password = os.environ['SMTP_PASSWORD']
    to_emails = os.environ['EMAIL_TO'].split(',')
    from_email = os.environ['EMAIL_FROM']

    msg = EmailMessage()
    msg['Subject'] = 'Test Email from GitHub Actions'
    msg['From'] = from_email
    msg['To'] = ', '.join(to_emails)
    msg.set_content('This is a test email with CSV attachments from GitHub Actions.')

    # Attach files
    for filename in ['simple_product_pricescrape.csv', 'child_product_pricescrape.csv']:
        with open(filename, 'rb') as f:
            file_data = f.read()
        msg.add_attachment(file_data, maintype='text', subtype='csv', filename=filename)

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(username, password)
        server.send_message(msg)

    print('Email sent successfully.')

if __name__ == '__main__':
    main()
