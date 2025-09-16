### **Tài Liệu Kỹ Thuật: Pipeline Xử Lý Dữ Liệu `Dimensions` (Tổng Quát)**

### **1. Tổng quan & Triết lý thiết kế 🧠**

Không giống như `Booking` là một pipeline đơn lẻ, pipeline `Dimensions` là một **khuôn mẫu (template)** được thiết kế để xử lý hàng loạt các bảng dữ liệu có chung một quy trình. Các bảng này (ví dụ: `Customers`, `Branches`, `Countries`) là các "chiều thông tin" giúp bổ sung ngữ cảnh cho dữ liệu `Fact`.

**Triết lý thiết kế:**

  * **"Cấu hình thay vì Code" (Configuration over Code):** Thay vì viết code riêng cho mỗi dimension, chúng ta định nghĩa chúng trong một file cấu hình duy nhất. Muốn thêm một dimension mới, ta chỉ cần thêm một mục vào file config, không cần viết thêm class Extractor hay Processor mới.
  * **Một luồng cho tất cả (One Workflow to Rule Them All):** Tất cả các dimension đều đi qua cùng một chuỗi các bước xử lý (Extract -\> Transform -\> Staging -\> Historical -\> DWH). Điều này giúp hệ thống cực kỳ dễ đoán, dễ bảo trì và dễ gỡ lỗi.
  * **Chất lượng dữ liệu là trên hết (Data Quality First):** Mọi record đều phải qua một bộ lọc làm sạch (`cleaner`) và một cổng kiểm soát chất lượng (`validator`) trước khi được xử lý tiếp. Dữ liệu xấu sẽ bị cách ly ngay lập tức.

-----

### **2. Workflow Chi Tiết & Giải Thích Function**

Đây là luồng dữ liệu mà một dimension (ví dụ: `customers`) sẽ đi qua.

#### **Giai đoạn 1: Configuration (Khai báo)** 📜

Đây là điểm khởi đầu của mọi thứ.

  * **File:** `src/data_pipeline/config/pipeline_config.py`
  * **Thành phần:** Biến `DIMENSIONS_CONFIG`
  * **Nhiệm vụ:** Đây là "bộ não" của pipeline. Nó là một danh sách, mỗi phần tử là một dictionary định nghĩa một dimension cần xử lý.

<!-- end list -->

```python
# Ví dụ một mục trong DIMENSIONS_CONFIG
{
    "name": "customers", # Tên định danh (sẽ được dùng để đặt tên file, folder)
    "endpoint": "customers", # Đường dẫn API
    "is_paginated": True, # API có phân trang không?
    "is_shared_across_branches": False, # Dữ liệu có dùng chung cho mọi chi nhánh không?
    "base_params": {"limit": 500}, # Tham số mặc định khi gọi API
    "schema": Customer, # Pydantic schema để xác thực
    "primary_key": "id", # Khóa chính của dimension
}
```

  * **Workflow:** Khi pipeline `dimensions` được gọi, `main.py` sẽ **lặp qua từng dictionary** trong `DIMENSIONS_CONFIG` và thực hiện toàn bộ chuỗi xử lý dưới đây cho từng cái.

-----

#### **Giai đoạn 2: Extract (Trích xuất Thông minh)** 🚚

  * **File:** `src/data_pipeline/pipelines/dimensions/extractor.py`
  * **Thành phần:** `DimensionExtractor`
  * **Nhiệm vụ:** Đọc một mục config (ví dụ: config của `customers`) và lấy dữ liệu thô từ PMS.

**Workflow bên trong `DimensionExtractor`:**

1.  **`__init__(self, config, client)`:** Extractor được khởi tạo với **config** của dimension nó chịu trách nhiệm và một **client** dùng chung.
2.  **`extract_multi_branch(...)`:**
      * Hàm này nhận được yêu cầu lấy dữ liệu cho tất cả các chi nhánh.
      * Nó đọc cờ `"is_shared_across_branches"` trong config.
          * **Nếu `True` (ví dụ: dimension `countries`):** Nó chỉ gọi API **một lần duy nhất** với chi nhánh đầu tiên. Sau khi có kết quả, nó "nhân bản" kết quả đó cho tất cả các chi nhánh khác. =\> **Tiết kiệm tài nguyên và thời gian.**
          * **Nếu `False` (ví dụ: dimension `customers`):** Nó sẽ gọi API song song cho **từng chi nhánh** một.
3.  **`_fetch_data_for_branch(...)`:**
      * Đây là hàm thực thi việc gọi API cho một chi nhánh.
      * Nó đọc cờ `"is_paginated"` trong config.
          * **Nếu `True`:** Gọi hàm `client.paginate_json(...)`, tự động lấy hết tất cả các trang dữ liệu.
          * **Nếu `False`:** Gọi hàm `client.get_json(...)` để lấy dữ liệu một lần.
      * Nó cũng đọc `"base_params"` từ config để truyền các tham số như `limit` vào API.

-----

#### **Giai đoạn 3: Transform & Validate (Làm sạch & Kiểm định)** ✨

Đây là "chốt kiểm soát chất lượng" quan trọng nhất.

  * **File:** `src/data_pipeline/pipelines/dimensions/transformer.py`
  * **Thành phần:** `GenericTransformer` và hàm helper `universal_cleaner`
  * **Nhiệm vụ:** Nhận dữ liệu thô từ Extractor, làm sạch, xác thực và phân loại chúng.

**Workflow bên trong `GenericTransformer.process(...)`:**

1.  Hàm `process` nhận một danh sách các record thô (ví dụ: 66,430 record `customers`).
2.  Nó lặp qua **từng record một** và thực hiện chuỗi `try/except`:
      * **`try` block:**
        a.  **`universal_cleaner(record, schema)`:** Record thô được đưa vào hàm `universal_cleaner` đầu tiên. Hàm này giống như một "bộ lọc sơ bộ", thực hiện các tác vụ:
        \* **Sửa lỗi `attachments`:** Nó thấy `attachments` là một `list` trong khi schema yêu cầu `string`. Nó sẽ tự động dùng `json.dumps()` để chuyển `list` này thành một chuỗi JSON hợp lệ.
        \* **Xử lý khóa ngoại:** Nó thấy `country_id` có dạng `[48, "China"]` và biết rằng chỉ cần lấy số `48`.
        \* **Chuẩn hóa giá trị:** Chuyển `False` thành `None`, điền giá trị mặc định cho các trường bắt buộc bị thiếu...
        b.  **`validated = self.schema(**cleaned_record)`:** Record đã được làm sạch sẽ được đưa vào Pydantic schema (`Customer`). Pydantic sẽ kiểm tra nghiêm ngặt: `id` có phải là số không? `dob` có phải là ngày hợp lệ không?
        c.  Nếu tất cả đều hợp lệ, record sẽ được thêm vào danh sách `clean_records`.
      * **`except ValidationError as e` block:**
        a.  Nếu Pydantic phát hiện bất kỳ sai sót nào, nó sẽ ném ra một lỗi `ValidationError`.
        b.  Chúng ta bắt lỗi này, lấy record **gốc** (chưa qua xử lý), thêm một cột mới là `validation_error` chứa thông báo lỗi chi tiết, và thêm nó vào danh sách `quarantine_records`.
3.  **Kết quả:** Hàm trả về một dictionary chứa 2 DataFrame: `{"clean": df_clean, "quarantine": df_quarantine}`.

-----

#### **Giai đoạn 4, 5, 6: Staging, Historical, DWH**

Luồng này giống hệt pipeline `Booking`.

  * **`StagingLoader`:** Nhận `df_clean` và lưu vào thư mục staging.
  * **`QuarantineLoader`:** Nhận `df_quarantine` và lưu vào thư mục quarantine.
  * **`HistoricalProcessor`:** So sánh dữ liệu mới trong staging với dữ liệu cũ để tìm ra thay đổi.
  * **`GenericDimProcessor`:**
      * **Nhiệm vụ:** Xây dựng bảng dimension cuối cùng trong DWH.
      * **`_read_current_data()`:** Đọc **chỉ những file historical của dimension đang xử lý** (ví dụ: chỉ `customers_history.parquet`) và lọc ra các record có `is_current = True`.
      * **`_enforce_schema_and_collect_errors()`:** Thực hiện một lần kiểm tra kiểu dữ liệu cuối cùng trên toàn bộ dữ liệu. Đây là **chốt kiểm soát chất lượng thứ hai**, giúp phát hiện các lỗi về kiểu dữ liệu (ví dụ: một cột `int` bị lẫn chuỗi ký tự) và báo cáo qua email.
      * **`process()`:** Đổi tên các cột theo chuẩn DWH (ví dụ: `id` -\> `customer_key`), loại bỏ trùng lặp và lưu file Parquet cuối cùng.

Với kiến trúc này, bạn có thể tự tin rằng bất kỳ dimension nào được khai báo trong `DIMENSIONS_CONFIG` cũng sẽ được xử lý một cách nhất quán, an toàn và hiệu quả.