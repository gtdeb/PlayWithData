import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email_validator import validate_email, EmailNotValidError

class sendEmail:
    def send_email(self, user_email, reset_token):
        try:
            # Email configuration
            gmail_user = 'bill6kholi9@gmail.com'
            gmail_password = 'pznw bwwb tjxi jenl'
            to = user_email
            subject = 'Password Reset Request'
            reset_link = f'http://localhost:3000/reset-password?token={reset_token}'

            # Email content
            text = f"""
            You are receiving this email because you requested a password reset.
            Please click the link below to reset your password:
            {reset_link}

            If you did not request this, please ignore this email.
            """
            html = f"""
            <html>
            <body>
                <p>You are receiving this email because you requested a password reset.</p>
                <p>Please click the link below to reset your password:</p>
                <p><a href="{reset_link}">Reset Password</a></p>
                <p>If you did not request this, please ignore this email.</p>
            </body>
            </html>
            """

            # Creating the message
            msg = MIMEMultipart('alternative')
            msg['From'] = gmail_user
            msg['To'] = to
            msg['Subject'] = subject

            part1 = MIMEText(text, 'plain')
            part2 = MIMEText(html, 'html')

            msg.attach(part1)
            msg.attach(part2)

            # Sending the email
            server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
            server.login(gmail_user, gmail_password)
            server.sendmail(gmail_user, to, msg.as_string())
            server.quit()

            print(f'Successfully sent password reset email to {user_email}')

        except EmailNotValidError as e:
            print(str(e))
            retStat = False
        except Exception as e:
            print(f'Failed to send email: {str(e)}')
            retStat = False
        else:
            retStat = True
        finally:
            return retStat
