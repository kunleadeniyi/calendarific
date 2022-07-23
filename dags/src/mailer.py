#  send mail
import smtplib 
from email.message import EmailMessage

# move to .env
gmail_address = 'developer.ayok@gmail.com'
gmail_password = 'ckjgfxjnsaropfml'

# create email body and message
msg = EmailMessage()
msg['From'] = gmail_address
msg['To'] = 'adeniyikunle22@gmail.com'
msg['Subject'] = 'Dine with US!'
msg.set_content('You are invited to dine with us as we celebrate the new year!')

def prepare_content(content):
    print(type(content))
    if isinstance(content, list):
        is_or_are = 'is' if len(content) ==1 else 'are'
        body = f"<p>Holiday(s) this month {is_or_are}</p> \n\t<p></p> \n"
        body += "\t<table border='1' style='border-collapse:collapse'> \n"
        body += """
            <tr>
                <th>Holiday</th>
                <th>Date</th>
                <th>Description</th>
            </tr>
        """
        for holiday in content:
            body += f"""
                <tr>
                    <th>{holiday.holiday}</th>
                    <th>{holiday.hol_date}</th>
                    <th>{holiday.description}</th>
                </tr>
            """
        body += "</table> \n"
        body += "\t<p></p> \n<p></p> \n<p>Best Regards</p>"

        return body

    return "Sorry, Must have run into an error"


def prepare_message(mail_to, subject, content, html = None, mail_from=gmail_address):
    msg = EmailMessage()
    msg['From'] = mail_from
    msg['To'] = mail_to
    msg['Subject'] = subject
    msg.set_content(content)
    if html is not None:
        msg.add_alternative(html, subtype='html')

    return msg

def send_mail(message: EmailMessage):
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        try:
            smtp.login(gmail_address, gmail_password)

            smtp.send_message(message)

        except Exception as error:
            print(str(error))


# For debuggin locally
# run this in terminal {python -m smtpd -c DebuggingServer -n localhost:1025}

"""
with smtplib.SMTP('localhost', 1025) as smtp:
    try:
        # smtp.login(gmail_address, gmail_password)
        
        subject = "Dine with Us"
        body = "You are invited to dine with us as we celebrate the new year!"

        message = f"Subject: {subject}\n\n {body}"

        smtp.sendmail(gmail_address, 'adeniyikunle22@gmail.com', message)

    except Exception as error:
        print(str(error))
"""