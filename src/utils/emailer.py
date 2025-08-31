# src/utils/emailer.py
from __future__ import annotations
import os
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Dict, Any

def send_etl_summary_email(
    *,
    subject: str,
    html_body: str,
    sender: str,
    password: str,
    recipient: str
):
    """
    Gửi email qua SMTP (Gmail App Password).
    """
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient

    part = MIMEText(html_body, "html", "utf-8")
    msg.attach(part)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender, password)
        server.sendmail(sender, [recipient], msg.as_string())


def render_summary_html(title: str, runs: List[Dict[str, Any]]) -> str:
    """
    Ráp HTML tổng hợp nhiều API/lần chạy.
    runs: [
      {
        "dataset": "bookings",
        "branch_id": 1,
        "records_out": 12345,
        "status": "success",
        "artifacts": [
          {"kind":"parquet","name":"...","link":"...","rows":100000},
          {"kind":"sheet","name":"...","link":"...","rows":50000},
        ]
      }, ...
    ]
    """
    rows_html = []
    for r in runs:
        arts_html = "".join([
            f'<div>- {a["kind"].upper()}: <a href="{a["link"]}">{a["name"]}</a> ({a["rows"]} dòng)</div>'
            for a in r.get("artifacts", [])
        ])
        rows_html.append(f"""
        <tr>
          <td>{r.get("dataset")}</td>
          <td>{r.get("branch_id")}</td>
          <td>{r.get("records_out")}</td>
          <td>{r.get("status")}</td>
          <td>{arts_html}</td>
        </tr>
        """)

    table = "".join(rows_html) or "<tr><td colspan='5'>(Không có dữ liệu)</td></tr>"

    return f"""
    <html><body>
      <h2>{title}</h2>
      <table border="1" cellpadding="6" cellspacing="0">
        <thead>
          <tr>
            <th>Dataset</th><th>Branch</th><th>Records</th><th>Status</th><th>Artifacts</th>
          </tr>
        </thead>
        <tbody>{table}</tbody>
      </table>
      <p style="color:#888">Email được tạo tự động từ ETL.</p>
    </body></html>
    """
