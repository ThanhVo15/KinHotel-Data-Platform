## **Tài Liệu Kỹ Thuật: Pipeline Xử Lý Dữ Liệu `Booking`**

### **1. Tổng quan & Mục tiêu 🎯**

Pipeline `Booking` là luồng xử lý dữ liệu quan trọng nhất, chịu trách nhiệm lấy toàn bộ thông tin đặt phòng từ hệ thống PMS, xử lý và chuyển đổi chúng thành các bảng dữ liệu sạch, có cấu trúc trong Data Warehouse (DWH) để phục vụ cho việc phân tích và báo cáo.

**Mục tiêu chính:**

  * **Trích xuất dữ liệu tăng trưởng (Incremental Extract):** Chỉ lấy những booking có thay đổi hoặc mới được tạo gần đây để giảm tải cho API.
  * **Lưu trữ lịch sử thay đổi (Slowly Changing Dimension - SCD Type 2):** Toàn bộ lịch sử thay đổi của một booking (ví dụ: đổi trạng thái, đổi giá) đều được ghi lại. Điều này cực kỳ quan trọng để phân tích "tại thời điểm đó".
  * **Tạo Bảng Dữ liệu Phân tích:** Xây dựng bảng `fact_booking` (chứa các sự kiện) và các bảng `dim_*` (chứa các chiều thông tin) sẵn sàng cho việc kết nối với các công cụ BI như Power BI hay Tableau.
  * **Tự động và Bền vững:** Pipeline được thiết kế để chạy tự động hàng ngày, có khả-năng tự phục hồi và báo cáo lỗi rõ ràng.

-----

### **2. Kiến trúc & Luồng Dữ liệu**

Dữ liệu `Booking` chảy qua 5 giai đoạn chính, mỗi giai đoạn có một vai trò và trách nhiệm riêng biệt:

1.  **Extract (Trích xuất):**

      * **Nhiệm vụ:** Kết nối tới API của PMS và lấy dữ liệu thô (dạng JSON).
      * **Thành phần:** `BookingExtractor` (`pipelines/booking/extractor.py`)
      * **Logic:** Sử dụng logic "delta" dựa trên lần chạy cuối cùng để chỉ lấy dữ liệu mới hoặc được cập nhật.

2.  **Parse (Phân tích & Làm phẳng):**

      * **Nhiệm vụ:** Chuyển đổi dữ liệu JSON thô (có thể lồng nhau phức tạp) thành một DataFrame (bảng) có cấu trúc phẳng và các cột được đặt tên rõ ràng.
      * **Thành phần:** `PMSBookingParser` (`pipelines/booking/parser.py`)
      * **Logic:** Áp dụng các quy tắc làm sạch, chuẩn hóa kiểu dữ liệu cơ bản.

3.  **Staging (Khu vực trung chuyển):**

      * **Nhiệm vụ:** Lưu trữ dữ liệu đã được làm phẳng từ bước Parse. Đây là "bản sao" của dữ liệu nguồn sau khi đã qua xử lý sơ bộ.
      * **Thành phần:** `StagingLoader` (`loaders/staging_loader.py`)
      * **Logic:** Dữ liệu được lưu dưới dạng file Parquet, phân vùng theo tháng và mã chi nhánh để tối ưu việc truy vấn.

4.  **Historical (Lưu trữ lịch sử):**

      * **Nhiệm vụ:** Đây là trái tim của pipeline. Nó so sánh dữ liệu mới ở Staging với dữ liệu của ngày hôm trước để xác định các thay đổi.
      * **Thành phần:** `HistoricalProcessor` (`core/historical_processor.py`)
      * **Logic:**
          * **Record Mới:** Được thêm vào với cờ `is_current = True`.
          * **Record Thay đổi:** Record cũ được "đóng lại" (cập nhật `valid_to`, `is_current = False`), và một phiên bản mới của record được thêm vào với `is_current = True`.
          * **Record Không đổi:** Được giữ nguyên.

5.  **Data Warehouse (Tổng hợp dữ liệu):**

      * **Nhiệm vụ:** Từ lớp Historical, lấy ra tất cả các record đang có hiệu lực (`is_current = True`) để xây dựng các bảng dữ liệu cuối cùng cho việc phân tích.
      * **Thành phần:** `FactBookingProcessor`, `DimDateProcessor`, `DimMarketProcessor` (`pipelines/booking/processors.py`)
      * **Logic:** Tạo ra các bảng Fact và Dimension sạch, tối ưu cho việc truy vấn.

-----

### **3. Hướng dẫn Code cho người mới**

Giả sử bạn cần tạo một pipeline mới tên là `Revenue` (Doanh thu), có logic tương tự `Booking`. Dưới đây là các bước bạn cần làm.

#### **Bước 1: Tạo cấu trúc thư mục**

Trong thư mục `src/data_pipeline/pipelines/`, hãy tạo một thư mục mới tên là `revenue`:

```plaintext
pipelines/
├── booking/
└── revenue/          <-- THƯ MỤC MỚI
    ├── __init__.py
    ├── extractor.py
    ├── parser.py
    └── processors.py
```

#### **Bước 2: Viết `extractor.py`**

Đây là lớp chịu trách nhiệm lấy dữ liệu từ API. Bạn chỉ cần kế thừa từ `PMSExtractor` và chỉ định `ENDPOINT`.

**File:** `pipelines/revenue/extractor.py`

```python
import aiohttp
from typing import Any, Dict, List
from ...core.pms_client import PMSClient
from ...core.pms_extractor import PMSExtractor

class RevenueExtractor(PMSExtractor):
    """Extractor chuyên dụng để lấy dữ liệu Revenue."""
    # Chỉ cần thay đổi ENDPOINT ở đây
    ENDPOINT = "revenue-details"

    def __init__(self, client: PMSClient):
        # Kế thừa hoàn toàn từ lớp cha
        super().__init__(client=client)

    async def _perform_extraction(self, session: aiohttp.ClientSession, branch_id: int, **kwargs) -> List[Dict[str, Any]]:
        # Lớp cha sẽ truyền `params` từ date_window vào kwargs
        params = kwargs.get("params", {})
        params.setdefault("limit", 100) # Có thể set limit mặc định
        
        # Gọi hàm phân trang của client
        return await self.client.paginate_json(session, self.ENDPOINT, base_params=params)
```

**Giải thích:** Bạn gần như không phải viết lại logic nào. Lớp `PMSExtractor` trong `core` đã xử lý toàn bộ logic phức tạp về lấy dữ liệu tăng trưởng, quản lý state và chạy song song.

#### **Bước 3: Viết `parser.py`**

Đây là nơi bạn định nghĩa cấu trúc dữ liệu và logic làm phẳng cho `Revenue`.

**File:** `pipelines/revenue/parser.py`

```python
import pandas as pd
from typing import List, Dict, Any
from ...core.abstract_parser import AbstractParser

class RevenueParser(AbstractParser):
    
    # Định nghĩa các cột bạn muốn có trong bảng cuối cùng
    COLUMNS = [
        "revenue_id", "booking_line_id", "date", "amount", 
        "payment_method", "branch_id", "extracted_at"
    ]

    def parse(self, data: List[Dict[str, Any]], **kwargs) -> pd.DataFrame:
        branch_id = kwargs.get("branch_id")
        
        # Viết một hàm helper để làm phẳng một record
        flattened_records = [self._flatten_record(rec, branch_id) for rec in data]
        
        df = pd.DataFrame(flattened_records)
        
        # Chuẩn hóa kiểu dữ liệu
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        
        # Đảm bảo tất cả các cột đều tồn tại
        for col in self.COLUMNS:
            if col not in df.columns:
                df[col] = None
        
        return df[self.COLUMNS]

    def _flatten_record(self, rec: Dict[str, Any], branch_id: int) -> Dict[str, Any]:
        # Đây là nơi bạn ánh xạ dữ liệu từ JSON thô sang các cột đã định nghĩa
        return {
            "revenue_id": rec.get("id"),
            "booking_line_id": rec.get("booking_line", {}).get("id"),
            "date": rec.get("payment_date"),
            "amount": rec.get("total_amount"),
            "payment_method": rec.get("method"),
            "branch_id": branch_id,
            "extracted_at": pd.Timestamp.utcnow()
        }
```

#### **Bước 4: Tích hợp vào `main.py`**

Cuối cùng, bạn cần "dạy" cho trình điều khiển `main.py` cách chạy pipeline mới này.

**File:** `src/data_pipeline/main.py`

1.  **Import** các thành phần mới của bạn.
2.  **Viết một hàm `run_revenue_pipeline`** bằng cách sao chép và chỉnh sửa từ `run_booking_pipeline`.
3.  **Thêm lựa chọn** vào `argparse` để có thể gọi pipeline từ dòng lệnh.

<!-- end list -->

```python
# Trong main.py

# 1. Import
from .pipelines.revenue.extractor import RevenueExtractor
from .pipelines.revenue.parser import RevenueParser
# ... import các processor của revenue nếu có

# 2. Viết hàm chạy
async def run_revenue_pipeline(client: PMSClient, config: dict, report: dict):
    logger.info("="*20 + " BẮT ĐẦU PIPELINE REVENUE " + "="*20)
    # ... Sao chép logic từ run_booking_pipeline và thay đổi các biến ...
    # Ví dụ: dataset="revenue", primary_key="revenue_id"
    extractor = RevenueExtractor(client=client)
    # ...
    
# 3. Thêm vào main
async def main():
    parser = argparse.ArgumentParser(...)
    # Thêm 'revenue' vào choices
    parser.add_argument("pipeline", choices=['booking', 'dimensions', 'revenue', 'all'], ...)
    args = parser.parse_args()
    
    # ...
    if args.pipeline in ['revenue', 'all']:
        await run_revenue_pipeline(client, config, report)
    # ...
```

Bằng cách tuân theo cấu trúc đã được thiết lập sẵn này, việc thêm một pipeline mới trở nên cực kỳ nhanh chóng và có hệ thống. Bạn chỉ cần tập trung vào logic nghiệp vụ cụ thể của pipeline đó (cách parse, các cột cần lấy) mà không cần lo lắng về các phần phức tạp như quản lý state, xử lý lịch sử hay điều phối song song.