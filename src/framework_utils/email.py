# utils/emailConfig.py

def email_to_name(email):
    parts = email.split("@")[0].split(".")
    return f"{parts[1].capitalize()}, {parts[0].capitalize()}" if len(parts) > 1 else email