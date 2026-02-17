import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def send_email(subject: str, html_body: str):
    host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER")
    password = os.getenv("SMTP_PASS")
    to_addr = os.getenv("ALERT_TO")

    if not all([user, password, to_addr]):
        raise RuntimeError("Missing SMTP_USER / SMTP_PASS / ALERT_TO in .env")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = to_addr

    msg.attach(MIMEText(html_body, "html", "utf-8"))

    with smtplib.SMTP(host, port) as server:
        server.starttls()
        server.login(user, password)
        server.sendmail(user, [to_addr], msg.as_string())
