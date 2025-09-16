# E:\Job\Kin-Hotel\DE\KinHotelAutoDashboard\src\data_pipeline\extractors\extract_metadata_manager.py
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any

class ExtractMetadataManager:
    """
    Quản lý metadata của các lần extract để hỗ trợ delta processing.
    
    Lưu trữ:
    - Thời điểm extract
    - Window_start, Window_end của mỗi lần extract
    - Số lượng records trích xuất
    - Các thông tin khác
    """
    
    def __init__(self, metadata_dir: str = "./data/metadata"):
        self.metadata_dir = Path(metadata_dir)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
    
    def save_extract_metadata(
        self,
        dataset: str,
        branch_id: int,
        extract_time: datetime,
        window_start: datetime,
        window_end: datetime,
        record_count: int,
        status: str = "success",
        error: Optional[str] = None,
        **additional_info
    ) -> str:
        """
        Lưu metadata của lần extract hiện tại.
        
        Returns: 
            Đường dẫn file metadata đã lưu
        """
        # Tạo thư mục cho dataset và branch
        branch_dir = self.metadata_dir / dataset / f"branch={branch_id}"
        branch_dir.mkdir(parents=True, exist_ok=True)
        
        # Xác định count_run
        count_run = 1
        latest_meta = self.get_latest_extract(dataset, branch_id)
        if latest_meta:
            # Nếu đã có metadata trước đó, tăng count_run lên 1
            count_run = latest_meta.get("count_run", 0) + 1
        
        # Tạo metadata object
        metadata = {
            "dataset": dataset,
            "branch_id": branch_id,
            "extract_time": extract_time.astimezone(timezone.utc).isoformat(),
            "window_start": window_start.astimezone(timezone.utc).isoformat(),
            "window_end": window_end.astimezone(timezone.utc).isoformat(),
            "record_count": record_count,
            "status": status,
            "count_run": count_run  # Thêm trường count_run
        }
        
        if error:
            metadata["error"] = error
            
        # Thêm thông tin bổ sung
        metadata.update(additional_info)
        
        # Tạo tên file
        timestamp = extract_time.astimezone(timezone.utc).strftime("%Y%m%d-%H%M%SZ")
        filename = f"extract_metadata_{timestamp}.json"
        filepath = branch_dir / filename
        
        # Lưu file
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
            
        return str(filepath)
    
    def append_to_combined_metadata(
        self,
        dataset: str,
        extract_time: datetime,
        branch_results: List[Dict[str, Any]],
        field: str = "",
        lookback_days: int = 0,
        additional_info: Dict[str, Any] = None
    ) -> str:
        """
        Lưu metadata của nhiều branch vào một file chung.
        
        Args:
            dataset: Tên dataset
            extract_time: Thời điểm bắt đầu extract
            branch_results: Danh sách kết quả extract theo branch
            field: Trường dùng để lọc dữ liệu
            lookback_days: Số ngày lookback
            additional_info: Thông tin bổ sung
            
        Returns:
            Đường dẫn file metadata đã lưu
        """
        # Tạo thư mục cho dataset
        dataset_dir = self.metadata_dir / dataset 
        dataset_dir.mkdir(parents=True, exist_ok=True)
        
        # Tạo metadata chung
        timestamp = extract_time.astimezone(timezone.utc).strftime("%Y%m%d-%H%M%SZ")
        combined_metadata = {
            "dataset": dataset,
            "extract_time": extract_time.astimezone(timezone.utc).isoformat(),
            "field": field,
            "lookback_days": lookback_days,
            "total_branches": len(branch_results),
            "total_records": sum(b.get("record_count", 0) for b in branch_results),
            "success_branches": sum(1 for b in branch_results if b.get("status") == "success"),
            "branches": branch_results
        }
        
        # Thêm thông tin bổ sung
        if additional_info:
            combined_metadata.update(additional_info)
        
        # Tạo tên file
        filename = f"extract_combined_{dataset}_{timestamp}.json"
        filepath = dataset_dir / filename
        
        # Lưu file
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(combined_metadata, f, indent=2, ensure_ascii=False)
        
        return str(filepath)
    
    def get_latest_extract(self, dataset: str, branch_id: int) -> Optional[Dict[str, Any]]:
        """Lấy metadata của lần extract gần nhất."""
        branch_dir = self.metadata_dir / dataset / f"branch={branch_id}"
        if not branch_dir.exists():
            return None
            
        # Lấy tất cả file metadata
        files = list(branch_dir.glob("extract_metadata_*.json"))
        if not files:
            return None
            
        # Sắp xếp theo thời gian tạo file (mới nhất cuối)
        files.sort(key=os.path.getmtime)
        latest_file = files[-1]
        
        # Đọc file
        try:
            with open(latest_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    
    def get_previous_extract(self, dataset: str, branch_id: int, current_time: datetime) -> Optional[Dict[str, Any]]:
        """Lấy metadata của lần extract trước đó."""
        branch_dir = self.metadata_dir / dataset / f"branch={branch_id}"
        if not branch_dir.exists():
            return None
            
        # Lấy tất cả file metadata
        files = list(branch_dir.glob("extract_metadata_*.json"))
        if len(files) < 2:  # Cần ít nhất 2 file
            return None
            
        # Sắp xếp theo thời gian tạo file (mới nhất cuối)
        files.sort(key=os.path.getmtime)
        
        # Lấy file cuối cùng
        latest_file = files[-1]
        
        # Đọc metadata hiện tại để kiểm tra
        try:
            with open(latest_file, "r", encoding="utf-8") as f:
                latest_meta = json.load(f)
                latest_time = datetime.fromisoformat(latest_meta["extract_time"])
                
            # Nếu không phải là current_time, trả về nó
            if abs((latest_time - current_time.astimezone(timezone.utc)).total_seconds()) > 60:
                return latest_meta
                
            # Nếu là current_time, lấy file trước đó
            previous_file = files[-2]
            with open(previous_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
            
    def get_latest_combined_extract(self, dataset: str) -> Optional[Dict[str, Any]]:
        """Lấy combined metadata gần nhất cho một dataset"""
        dataset_dir = self.metadata_dir / dataset
        if not dataset_dir.exists():
            return None
            
        # Lấy tất cả file combined metadata
        files = list(dataset_dir.glob("extract_combined_*.json"))
        if not files:
            return None
            
        # Sắp xếp theo thời gian tạo file (mới nhất cuối)
        files.sort(key=os.path.getmtime)
        latest_file = files[-1]
        
        # Đọc file
        try:
            with open(latest_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None