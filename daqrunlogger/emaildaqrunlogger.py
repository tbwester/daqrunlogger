# logger to send email to shifter slack channel once DAQ run ends

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from .daqrunlogger import RunInfo

class EmailDAQRunLogger:
    
    def __init__(
        self,
        sender: str = "sbnddaq",
        recipients: str = "sbnd-shift-operations-aaaak3ro3cjdguez5l7glmobwu@shortbaseline.slack.com",
        #recipients: str = "pgreen@FNAL.GOV",
        smtp_host: str = "localhost",
    ):
        self._sender = sender
        self._recipients = recipients
        self._smtp_host = smtp_host

    def filter_run(self, info: RunInfo) -> bool:
        return info.end_time is not None

    def log_run(self, info: RunInfo) -> None:
        message = MIMEMultipart()
        message["From"] = self._sender
        message["To"] = self._recipients
        message["Subject"] = f"DAQ Run {info.run_number} Ended"

        body = (
            f"DAQ run {info.run_number} ended.\n"
            f"Start time: {info.start_time}\n"
            f"End time: {info.end_time}\n"
        )
        message.attach(MIMEText(body + "\n", "plain"))

        smtp = smtplib.SMTP(self._smtp_host)
        try:
            smtp.sendmail(
                self._sender,
                self._recipients.split(","),
                message.as_string(),
            )
        finally:
            smtp.quit()

