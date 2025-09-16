import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import pandas as pd
import io
from typing import List, Dict
from pathlib import Path

from .abstract_loader import AbstractLoader, LoadingResult

try:
    from jinja2 import Environment, FileSystemLoader
    JINJA2_AVAILABLE = True
except ImportError:
    JINJA2_AVAILABLE = False

class EmailNotifier(AbstractLoader):
    """Gửi email báo cáo, có khả năng đính kèm báo cáo chất lượng dữ liệu."""
    def __init__(self, smtp_server: str, port: int, sender: str, password: str, recipient: str):
        super().__init__("EmailNotifier")
        if not JINJA2_AVAILABLE:
            raise ImportError("Jinja2 not found. Please run 'pip install Jinja2'")
        
        self.smtp_server = smtp_server
        self.port = port
        self.sender = sender
        self.password = password
        self.recipient = recipient
        
        template_dir = Path(__file__).parent / "templates"
        self.jinja_env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)

    def _build_html_report(self, report_data: dict) -> str:
        template = self.jinja_env.get_template("report.template.html")
        return template.render(report=report_data)

    def _create_dq_attachment(self, dq_issues: List[Dict]) -> MIMEBase:
        """Tạo file báo cáo chất lượng dữ liệu (CSV) để đính kèm."""
        if not dq_issues:
            return None
        
        df = pd.DataFrame(dq_issues)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
        
        attachment = MIMEBase('application', 'octet-stream')
        attachment.set_payload(csv_buffer.getvalue().encode('utf-8'))
        encoders.encode_base64(attachment)
        attachment.add_header('Content-Disposition', 'attachment; filename="data_quality_report.csv"')
        
        return attachment

    def load(self, report_data: dict) -> LoadingResult:
        result = LoadingResult(name=self.name, target_location=self.recipient)
        
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"Pipeline Report [{report_data['overall_status']}] - {report_data['execution_date']}"
        msg['From'] = self.sender
        msg['To'] = self.recipient

        try:
            html_body = self._build_html_report(report_data)
            msg.attach(MIMEText(html_body, 'html'))
            
            dq_issues = report_data.get("data_quality_issues")
            if dq_issues:
                self.logger.info(f"Đang tạo báo cáo cho {len(dq_issues)} vấn đề chất lượng dữ liệu...")
                dq_attachment = self._create_dq_attachment(dq_issues)
                if dq_attachment:
                    msg.attach(dq_attachment)
            
            with smtplib.SMTP(self.smtp_server, self.port) as server:
                server.starttls()
                server.login(self.sender, self.password)
                server.sendmail(self.sender, self.recipient.split(','), msg.as_string())
                self.logger.info("✅ Email report sent successfully.")
            result.files_uploaded = 1
        except Exception as e:
            self.logger.exception(f"Failed to send email report: {e}")
            result.status = "error"
            result.error = str(e)
            
        return result