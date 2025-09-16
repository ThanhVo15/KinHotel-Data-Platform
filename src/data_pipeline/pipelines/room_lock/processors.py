import pandas as pd
from pathlib import Path

# --- Import các thành phần từ core ---
from ...core.dwh_processor import DWHModelProcessor
from ...core.abstract_processor import ProcessingResult
from ...schemas.pms_schemas import RoomLock

class FactRoomLockProcessor(DWHModelProcessor):
    def __init__(self, execution_historical_dir: Path, dwh_dir: Path):
        # Khai báo processor này xử lý dataset "room_lock"
        super().__init__(
            "FactRoomLockProcessor",
            execution_historical_dir,
            dwh_dir,
            dataset_name="room_lock"
        )

    def process(self) -> ProcessingResult:
        # 1. Đọc dữ liệu historical hiện hành (`is_current = True`)
        df = self._read_current_data()
        if df.empty:
            return ProcessingResult(name=self.name, status="skipped", error="No input data for FactRoomLock.")

        self.logger.info("Creating final FactRoomLock table...")
        fact_df = df.copy()
        
        # 2. Lấy danh sách cột từ Pydantic model để đảm bảo schema nhất quán
        final_columns = list(RoomLock.model_fields.keys())
        
        # Thêm các cột metadata quan trọng vào bảng fact
        final_columns.extend(['branch_id', 'valid_from', 'extracted_at'])

        # Chỉ giữ lại các cột có trong DataFrame để tránh lỗi
        existing_cols = [col for col in final_columns if col in fact_df.columns]
        final_df = fact_df[existing_cols]

        # 3. Lưu bảng DWH
        output_path = self.dwh_dir / "fact_room_lock" / "fact_room_lock.parquet"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        final_df.to_parquet(output_path, index=False)
        self.logger.info(f"Saved FactRoomLock to {output_path}")

        return ProcessingResult(
            name=self.name, 
            input_records=len(df), 
            output_records=len(final_df), 
            output_tables=[str(output_path)]
        )