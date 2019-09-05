# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------
import smtplib
from email.message import EmailMessage
import re


class EmailNotification(object):

    def __init__(self, **params):
        self._port = params.get("smtpport")  
        self._smtp_server = params.get("smtpendpoint")
        self._sender_email = params.get("sendfrom")
        self._receiver_email = params.get("sendto") 
        self._password = params.get("sendfrompassword")
        self._context = params.get("context")

        self._validate_email_params()


    @property
    def send_to(self):
        return self._receiver_email

    @property
    def context(self):
        return self._context or 'unknwon'


    def send_email(self, subject, message):
        # context = ssl.create_default_context()
        # with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:

        with smtplib.SMTP(self._smtp_server, self._port) as server:
            server.starttls() 
            server.login(self._sender_email, self._password)
            result = server.sendmail(self._sender_email, self._receiver_email, 
            f"From: {self._sender_email}\nTo: {self._receiver_email}\nSubject: {subject}\n\nContext: {self.context}\n{message}")
            if result:
                error_message = []
                for key, value in result:
                    error_message.append(f"failed to send mail to {key} due to error {value}")
                raise ValueError(",\n".join(error_message))


    def _validate_email_params(self):
        if self._port and self._smtp_server and self._sender_email and self._receiver_email and self._password:
            if self._is_email_format(self._sender_email) and self._is_email_format(self._receiver_email):
                return True
        raise ValueError("""
            cannot notify device_code login by email because some email parameters are missing. 
            Set KQLMAGIC_CODE_NOTIFICATION_EMAIL in the following way: SMTPEndPoint: \" email server\"; SMTPPort: \"email port\";
            sendFrom: \"sender email address \"; sendFromPassword: \"email address password \"; sendTo:\" email address to send to\"""" )


    def _is_email_format(self, email):
        return re.match( r'[\w\.-]+@[\w\.-]+(\.[\w]+)+', email)
    
