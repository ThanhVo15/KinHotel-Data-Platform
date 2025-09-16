## Quy trình Xử lý Dữ liệu - Giai đoạn 2: Lớp Lịch sử (Historical Layer)

### 1\. Tổng quan

Tài liệu này mô tả chi tiết Giai đoạn 2 của pipeline, tập trung vào việc biến đổi dữ liệu thô từ **Staging Layer** thành một **Historical Layer** có cấu trúc và đáng tin cậy.

Mục tiêu chính của giai đoạn này là xây dựng một bản ghi lịch sử đầy đủ cho mỗi thực thể (ví dụ: mỗi booking), theo dõi mọi sự thay đổi của nó qua thời gian. Kiến trúc này sử dụng phương pháp **Slowly Changing Dimension Type 2 (SCD Type 2)**, được triển khai dưới dạng **"Ảnh chụp dữ liệu hàng ngày" (Daily Snapshot)**.

-----

### 2\. Mục tiêu chính 🎯

  * **Theo dõi Lịch sử Thay đổi (SCD Type 2):** Mỗi bản ghi trong lớp Historical sẽ có các cột metadata (`valid_from`, `valid_to`, `is_current`) để xác định chính xác phiên bản nào của dữ liệu là "hiện tại" và nó có hiệu lực trong khoảng thời gian nào.
  * **Dữ liệu Bất biến (Immutability):** Mỗi snapshot hàng ngày là một bản ghi vĩnh viễn, không thể thay đổi về trạng thái của tất cả các booking tính đến cuối ngày hôm đó. Lịch sử không bao giờ bị ghi đè.
  * **Làm sạch và Chuẩn hóa Dữ liệu:** Đây là bước đầu tiên mà dữ liệu được làm sạch một cách có hệ thống. Các kiểu dữ liệu (ngày tháng, số nguyên,...) được ép kiểu một cách nhất quán để đảm bảo tính toàn vẹn trước khi đưa vào các bước xử lý sâu hơn.

-----

### 3\. Kiến trúc Luồng Dữ liệu 🏗️

Quy trình này được thực thi bởi `HistoricalProcessor` cho từng chi nhánh, bao gồm các bước:

#### 3.1. Đọc Dữ liệu Nguồn

Processor đọc dữ liệu từ hai nơi:

1.  **Trạng thái mới nhất từ Staging:** Đọc toàn bộ file log thô của tháng hiện tại, sau đó sắp xếp và loại bỏ trùng lặp để có được phiên bản mới nhất của mỗi booking.
2.  **Snapshot lịch sử của ngày hôm trước:** Đọc file `booking_history.parquet` từ thư mục của ngày hôm qua (`historical/{hôm_qua}/...`).

#### 3.2. So sánh và Phát hiện Thay đổi

Dữ liệu từ hai nguồn trên được hợp nhất (merge) dựa trên khóa chính (`booking_line_id`) để phân loại các bản ghi thành 3 nhóm:

  * **Bản ghi mới (New):** Chỉ tồn tại trong Staging.
  * **Bản ghi không đổi (Unchanged):** Tồn tại ở cả hai nơi và các cột quan trọng không có gì thay đổi.
  * **Bản ghi thay đổi (Changed):** Tồn tại ở cả hai nơi nhưng có ít nhất một cột giá trị đã khác đi (ví dụ: `price`, `status`).

#### 3.3. Áp dụng Logic SCD Type 2

Dựa trên 3 nhóm trên, một DataFrame mới được tạo ra:

  * **Unchanged:** Được giữ nguyên từ snapshot của ngày hôm trước.
  * **Changed:**
      * Phiên bản cũ trong snapshot hôm qua sẽ được "đóng lại" bằng cách cập nhật cột `valid_to` thành thời gian hiện tại và `is_current` thành `False`.
      * Phiên bản mới từ Staging sẽ được thêm vào với `valid_from` là thời gian hiện tại và `is_current` là `True`.
  * **New:** Được thêm vào như một bản ghi hoàn toàn mới với `valid_from` là thời gian hiện tại và `is_current` là `True`.

#### 3.4. Ghi Snapshot Mới

Toàn bộ DataFrame kết quả sẽ được ghi vào một thư mục **mới** theo ngày của hôm nay (`historical/{hôm_nay}/...`), tạo ra một snapshot lịch sử hoàn chỉnh.

-----

### 4\. Cấu trúc Thư mục Historical

```
{HISTORICAL_DIR}/
└── YYYY/
    └── MM/
        └── DD/
            ├── branch=1/
            │   └── booking_history.parquet
            ├── branch=2/
            │   └── booking_history.parquet
            └── _combined_booking_history.parquet  <-- File gộp tạm thời
```

-----

### 5\. Ví dụ Minh họa

Tiếp nối ví dụ từ Giai đoạn 1, chúng ta sẽ xem lớp Historical được tạo ra như thế nào.

#### Ngày 1: 2025-09-08

  * **Kết quả file `historical/2025/09/08/branch=1/booking_history.parquet`:**
    | booking\_line\_id | price | status | valid\_from | valid\_to | is\_current |
    | :--- | :--- | :--- | :--- | :--- | :--- |
    | 101 | 500 | CONFIRMED | 2025-09-08 10:00 | NaT | **True** |
    | 102 | 800 | CONFIRMED | 2025-09-08 10:00 | NaT | **True** |

#### Ngày 2: 2025-09-09 (Booking mới 103)

  * **Kết quả file `historical/2025/09/09/branch=1/booking_history.parquet`:**
    | booking\_line\_id | price | status | valid\_from | valid\_to | is\_current |
    | :--- | :--- | :--- | :--- | :--- | :--- |
    | 101 | 500 | CONFIRMED | 2025-09-08 10:00 | NaT | **True** |
    | 102 | 800 | CONFIRMED | 2025-09-08 10:00 | NaT | **True** |
    | **103** | **1200** | **CONFIRMED** | **2025-09-09 10:00** | **NaT** | **True** |

#### Ngày 3: 2025-09-10 (Booking 102 thay đổi giá)

  * **Kết quả file `historical/2025/09/10/branch=1/booking_history.parquet`:**
    | booking\_line\_id | price | status | valid\_from | valid\_to | is\_current |
    | :--- | :--- | :--- | :--- | :--- | :--- |
    | 101 | 500 | CONFIRMED | 2025-09-08 10:00 | NaT | **True** |
    | 102 | 800 | CONFIRMED | 2025-09-08 10:00 | **2025-09-10 10:00** | **False** |
    | 103 | 1200 | CONFIRMED | 2025-09-09 10:00 | NaT | **True** |
    | **102** | **950** | **CONFIRMED** | **2025-09-10 10:00** | **NaT** | **True** |

-----

### 6\. Kết quả cuối cùng

Giai đoạn 2 tạo ra một tài sản dữ liệu lịch sử toàn diện và đáng tin cậy. Nó là "cỗ máy thời gian", ghi lại mọi thay đổi và là đầu vào duy nhất cho Giai đoạn 3.
