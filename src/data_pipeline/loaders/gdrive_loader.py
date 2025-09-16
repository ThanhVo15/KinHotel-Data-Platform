# src/data_pipeline/loaders/gdrive_loader.py
from pathlib import Path
import io
from .abstract_loader import AbstractLoader, LoadingResult

# Import các thư viện mới của Google
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

class GoogleDriveLoader(AbstractLoader):
    """Tải các thư mục dữ liệu lên Google Drive, HỖ TRỢ SHARED DRIVE."""
    
    SCOPES = ['https://www.googleapis.com/auth/drive']

    def __init__(self, sa_path: str, root_folder_id: str):
        super().__init__("GoogleDriveLoader")
        self.root_folder_id = root_folder_id
        try:
            creds = Credentials.from_service_account_file(sa_path, scopes=self.SCOPES)
            self.service = build('drive', 'v3', credentials=creds)
            self.logger.info("✅ Google Drive authentication successful.")
        except Exception as e:
            self.logger.error(f"🔥 Google Drive authentication failed: {e}")
            raise e
        
        self._folder_cache = {}

    def _create_or_get_folder(self, parent_folder_id: str, folder_name: str) -> str:
        """Tìm một thư mục con, nếu không có thì tạo mới. Trả về ID."""
        cache_key = f"{parent_folder_id}/{folder_name}"
        if cache_key in self._folder_cache:
            return self._folder_cache[cache_key]

        query = f"'{parent_folder_id}' in parents and name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        
        # SỬA LỖI: Thêm supportsAllDrives và includeItemsFromAllDrives
        response = self.service.files().list(
            q=query, 
            spaces='drive', 
            fields='files(id, name)',
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        files = response.get('files', [])
        
        if files:
            folder_id = files[0].get('id')
            self._folder_cache[cache_key] = folder_id
            return folder_id
        else:
            self.logger.info(f"Folder '{folder_name}' not found, creating it...")
            file_metadata = {
                'name': folder_name,
                'parents': [parent_folder_id],
                'mimeType': 'application/vnd.google-apps.folder'
            }
            # SỬA LỖI: Thêm supportsAllDrives khi tạo folder
            folder = self.service.files().create(
                body=file_metadata, 
                fields='id',
                supportsAllDrives=True
            ).execute()
            folder_id = folder.get('id')
            self._folder_cache[cache_key] = folder_id
            return folder_id

    def load(self, local_path: str, target_subfolder: str) -> LoadingResult:
        local_dir = Path(local_path)
        if not local_dir.is_dir():
            return LoadingResult(name=self.name, status="error", error=f"Local path not a directory: {local_path}")

        target_folder_id = self._create_or_get_folder(self.root_folder_id, target_subfolder)
        result = LoadingResult(name=self.name, target_location=f"Drive Folder ID: {target_folder_id}")

        files_to_process = sorted([f for f in local_dir.rglob('*') if f.is_file()])
        self.logger.info(f"Found {len(files_to_process)} files to process in '{local_path}'.")

        for local_file in files_to_process:
            relative_path = local_file.relative_to(local_dir)
            current_parent_id = target_folder_id
            
            for part in relative_path.parts[:-1]:
                current_parent_id = self._create_or_get_folder(current_parent_id, part)

            file_name = local_file.name
            
            # SỬA LỖI: Thêm supportsAllDrives và includeItemsFromAllDrives khi tìm file
            query = f"'{current_parent_id}' in parents and name='{file_name}' and trashed=false"
            response = self.service.files().list(
                q=query, 
                spaces='drive', 
                fields='files(id, name)',
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()
            existing_files = response.get('files', [])
            
            media = MediaFileUpload(str(local_file), resumable=True)
            
            if existing_files: # File đã tồn tại -> Update
                file_id = existing_files[0].get('id')
                self.logger.info(f"Updating file: {relative_path}...")
                # SỬA LỖI: Thêm supportsAllDrives khi update
                self.service.files().update(
                    fileId=file_id, 
                    media_body=media,
                    supportsAllDrives=True
                ).execute()
                result.files_updated += 1
            else: # File chưa có -> Create
                self.logger.info(f"Uploading new file: {relative_path}...")
                file_metadata = {'name': file_name, 'parents': [current_parent_id]}
                # SỬA LỖI: Thêm supportsAllDrives khi tạo file
                self.service.files().create(
                    body=file_metadata, 
                    media_body=media, 
                    fields='id',
                    supportsAllDrives=True
                ).execute()
                result.files_uploaded += 1

            result.bytes_transferred += local_file.stat().st_size
            
        return result