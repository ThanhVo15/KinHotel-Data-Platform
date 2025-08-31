# src/utils/gdrive_client.py
from __future__ import annotations
import io
import json
import os
import logging
from typing import Dict, List, Optional
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/spreadsheets"  # để Drive convert CSV -> Sheets
]

MIME_FOLDER = "application/vnd.google-apps.folder"
MIME_SHEET  = "application/vnd.google-apps.spreadsheet"
MIME_OCTET  = "application/octet-stream"   # dùng an toàn cho parquet
MIME_CSV    = "text/csv"

class GoogleDriveClient:
    def __init__(self, root_folder_id: str):
        self.root_folder_id = root_folder_id
        self.service = self._build_service()

    def _build_service(self):
        creds = None
        sa_json = os.getenv("GDRIVE_SA_JSON")
        sa_path = os.getenv("GDRIVE_SA_PATH")

        if sa_json:
            info = json.loads(sa_json)
            creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
        elif sa_path:
            creds = service_account.Credentials.from_service_account_file(sa_path, scopes=SCOPES)
        else:
            raise RuntimeError("Missing Google SA credentials: set GDRIVE_SA_JSON or GDRIVE_SA_PATH")

        return build("drive", "v3", credentials=creds, cache_discovery=False)

    # ---------- Folders ----------
    def ensure_folder_path(self, parts: List[str]) -> str:
        """
        Ensure nested folders exist under root.
        Returns folder_id of the deepest folder.
        """
        parent = self._validate_root(self.root_folder_id)  # NEW
        for name in parts:
            parent = self._ensure_child_folder(parent, name)
        return parent

    def _validate_root(self, rid: str) -> str:
        """Kiểm tra root id tồn tại (kể cả Shared Drive)."""
        try:
            # fields tối thiểu đủ dùng
            f = self.service.files().get(
                fileId=rid,
                fields="id, name, mimeType",
                supportsAllDrives=True
            ).execute()
            return f["id"]
        except Exception as e:
            raise RuntimeError(
                f"GDRIVE_FOLDER_ID invalid or not shared with Service Account: {rid}. "
                f"Ensure SA is a member of the Shared Drive (or folder) and supportsAllDrives enabled. Err={e}"
            )

    def _ensure_child_folder(self, parent_id: str, name: str) -> str:
        q = (
            f"'{parent_id}' in parents and mimeType='{MIME_FOLDER}' "
            f"and name='{name}' and trashed=false"
        )
        res = self.service.files().list(
            q=q,
            fields="files(id, name)",
            includeItemsFromAllDrives=True,  # NEW
            supportsAllDrives=True           # NEW
        ).execute()
        files = res.get("files", [])
        if files:
            return files[0]["id"]

        file_metadata = {"name": name, "mimeType": MIME_FOLDER, "parents": [parent_id]}
        f = self.service.files().create(
            body=file_metadata,
            fields="id",
            supportsAllDrives=True           # NEW
        ).execute()
        logger.info(f"Created folder '{name}' under {parent_id}: {f['id']}")
        return f["id"]

    def upload_parquet_bytes(self, data: bytes, folder_id: str, filename: str) -> Dict[str, str]:
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype=MIME_OCTET, resumable=True)
        file_metadata = {"name": filename, "parents": [folder_id]}
        f = self.service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id, name",
            supportsAllDrives=True           # NEW
        ).execute()
        return {
            "id": f["id"],
            "name": f["name"],
            "webViewLink": f"https://drive.google.com/file/d/{f['id']}/view",
            "type": "parquet"
        }

    def upload_csv_as_sheet(self, csv_bytes: bytes, folder_id: str, filename: str) -> Dict[str, str]:
        media = MediaIoBaseUpload(io.BytesIO(csv_bytes), mimetype=MIME_CSV, resumable=True)
        file_metadata = {"name": filename, "parents": [folder_id], "mimeType": MIME_SHEET}
        f = self.service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id, name",
            supportsAllDrives=True           # NEW
        ).execute()
        return {
            "id": f["id"],
            "name": f["name"],
            "webViewLink": f"https://docs.google.com/spreadsheets/d/{f['id']}/edit",
            "type": "sheet"
        }
