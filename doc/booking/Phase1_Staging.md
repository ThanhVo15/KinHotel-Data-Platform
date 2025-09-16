# Quy trình Trích xuất Dữ liệu - Giai đoạn 1: Staging

## 1\. Tổng quan

Tài liệu này mô tả chi tiết Giai đoạn 1 (Phase 1) của hệ thống pipeline dữ liệu tại KinHotel. Mục tiêu của giai đoạn này là trích xuất dữ liệu thô từ các hệ thống nguồn (cụ thể là PMS API) và tải vào một khu vực lưu trữ trung gian gọi là **Staging Layer**.

Quy trình này được thiết kế để ưu tiên **độ tin cậy**, **khả năng kiểm toán (audit)**, và **khả năng phục hồi**. Dữ liệu ở lớp Staging là một bản ghi log đầy đủ, không thay đổi (append-only) của tất cả những gì API đã trả về theo thời gian.

-----

## 2\. Mục tiêu chính 🎯

  * **Tạo bản ghi log thô (Raw Log):** File ở Staging là một bản sao chính xác của dữ liệu được trích xuất tại một thời điểm cụ thể. Nó chứa cả dữ liệu trùng lặp qua các lần chạy, phản ánh đúng bản chất của việc lấy dữ liệu theo cửa sổ thời gian trôi (sliding window).
  * **Khả năng Audit & Retry:** Dữ liệu thô này cho phép chúng ta kiểm tra lại chính xác API đã trả về gì vào một ngày cụ thể. Nếu các bước xử lý sau (Phase 2: Historical) gặp lỗi, chúng ta có thể chạy lại từ Staging mà không cần phải gọi lại API, giảm tải cho hệ thống nguồn.
  * **Tách biệt quy trình (Separation of Concerns):** Giai đoạn này chỉ tập trung vào một việc duy nhất: lấy dữ liệu và lưu trữ an toàn. Toàn bộ logic làm sạch, biến đổi, và nghiệp vụ sẽ được thực hiện ở các giai đoạn sau.

-----

## 3\. Kiến trúc Luồng Dữ liệu 🏗️

Quy trình được tự động hóa và bao gồm các bước chính sau:

### 3.1. Khởi tạo & Điều phối

Một tiến trình (ví dụ: `main.py`, một job của Airflow) sẽ khởi tạo một `Extractor` cụ thể (ví dụ: `BookingListExtractor`). Tiến trình này sẽ kích hoạt việc trích xuất dữ liệu cho tất cả các chi nhánh được cấu hình.

### 3.2. Quản lý Cửa sổ Thời gian 🔄

Với mỗi chi nhánh, pipeline sẽ xác định khoảng thời gian cần lấy dữ liệu.

  * **Logic hiện tại:** Dựa trên trường `check_in_date`. Pipeline sẽ luôn lấy các booking có ngày check-in trong vòng **30 ngày gần nhất** so với thời điểm chạy.
  * **Ví dụ:** Nếu pipeline chạy vào ngày 09/09, nó sẽ yêu cầu API trả về tất cả booking có `check_in_date` từ ngày 10/08 đến 09/09.

### 3.3. Trích xuất & Phân trang

`PMSClient` sẽ thực hiện các cuộc gọi đến API nguồn với cửa sổ thời gian đã xác định. Nó tự động xử lý việc phân trang (pagination) để lấy toàn bộ các bản ghi thỏa mãn điều kiện.

### 3.4. Ghi Log Metadata

Mỗi lần chạy trích xuất (cho từng chi nhánh) sẽ tạo ra một file metadata JSON riêng biệt. File này ghi lại các thông tin quan trọng như:

  * Thời điểm bắt đầu/kết thúc trích xuất.
  * Cửa sổ thời gian (`window_start`, `window_end`).
  * Số lượng bản ghi lấy được.
  * Trạng thái (thành công/thất bại) và thông báo lỗi (nếu có).

### 3.5. Tải vào Staging 💾

Đây là bước cốt lõi của Giai đoạn 1.

  * **Định dạng:** Dữ liệu được lưu dưới dạng file Parquet (mặc định) hoặc CSV.
  * **Cấu trúc thư mục:** Dữ liệu được phân vùng theo hệ thống, tháng, chi nhánh và dataset để dễ dàng quản lý.
  * **Logic Ghi (Append-Only):**
    1.  Pipeline xác định file đích cho tháng hiện tại (ví dụ: `pms/202509/branch=1/fact_booking/bookings.parquet`).
    2.  Nó đọc toàn bộ nội dung của file này (nếu đã tồn tại).
    3.  Nó **nối (concatenate)** dữ liệu mới vừa trích xuất được vào cuối dữ liệu cũ.
    4.  Nó ghi đè lại toàn bộ file với nội dung đã được nối.

> **QUAN TRỌNG:** Ở giai đoạn này **KHÔNG** có bất kỳ thao tác làm sạch, biến đổi hay loại bỏ trùng lặp (deduplication) nào. File Staging là một bản ghi log đầy đủ.

-----

## 4\. Cấu trúc Thư mục Staging

```
{STAGING_DIR}/
└── pms/
    └── 202509/  <-- Tháng (Partition)
        ├── branch=1/
        │   └── fact_booking/
        │       └── fact_booking_create_branch=1_monthly.parquet
        └── branch=2/
            └── fact_booking/
                └── fact_booking_create_branch=2_monthly.parquet
```

-----

## 5\. Ví dụ Minh họa: Luồng Dữ liệu qua 4 ngày

Để hiểu rõ cách file Staging phát triển, hãy xem ví dụ sau. Chúng ta chỉ tập trung vào file `.../branch=1/.../bookings.parquet`.

#### Dữ liệu mẫu ban đầu:

  * **Booking 101**: Một booking ổn định.
  * **Booking 102**: Một booking sẽ thay đổi giá.

### Ngày 1

  * **API trả về:** 2 booking (101, 102).
  * **Kết quả file `staging_log.parquet`:**

| booking\_id | price | status    | extracted\_at        |
| :--------- | :---- | :-------- | :------------------ |
| 101        | 500   | CONFIRMED | 2025-09-01 09:00:00 |
| 102        | 800   | CONFIRMED | 2025-09-01 09:00:00 |

### Ngày 2

  * **API trả về:** 3 booking (101, 102, và booking mới 103).
  * **Kết quả file `staging_log.parquet`:** Dữ liệu mới được nối vào cuối file cũ.

| booking\_id | price | status    | extracted\_at        |
| :--------- | :---- | :-------- | :------------------ |
| 101        | 500   | CONFIRMED | 2025-09-01 09:00:00 |
| 102        | 800   | CONFIRMED | 2025-09-01 09:00:00 |
| **101** | **500** | **CONFIRMED** | **2025-09-02 09:00:00** |
| **102** | **800** | **CONFIRMED** | **2025-09-02 09:00:00** |
| **103** | **1200** | **CONFIRMED** | **2025-09-02 09:00:00** |

### Ngày 3

  * **API trả về:** 3 booking, nhưng giá của booking 102 đã **thay đổi thành 950**.
  * **Kết quả file `staging_log.parquet`:**

| booking\_id | price | status    | extracted\_at        |
| :--------- | :---- | :-------- | :------------------ |
| ...        | ...   | ...       | ...                 | (5 dòng cũ từ Ngày 1 & 2)
| **101** | **500** | **CONFIRMED** | **2025-09-03 09:00:00** |
| **102** | **950** | **CONFIRMED** | **2025-09-03 09:00:00** |
| **103** | **1200** | **CONFIRMED** | **2025-09-03 09:00:00** |

### Ngày 4

  * **API trả về:** 3 booking, nhưng status của booking 101 **thay đổi thành `CANCELLED`**.
  * **Kết quả file `staging_log.parquet`:**

| booking\_id | price | status    | extracted\_at        |
| :--------- | :---- | :-------- | :------------------ |
| ...        | ...   | ...       | ...                 | (8 dòng cũ từ Ngày 1, 2 & 3)
| **101** | **500** | **CANCELLED** | **2025-09-04 09:00:00** |
| **102** | **950** | **CONFIRMED** | **2025-09-04 09:00:00** |
| **103** | **1200** | **CONFIRMED** | **2025-09-04 09:00:00** |

-----

## 6\. Kết quả cuối cùng

Sau khi Giai đoạn 1 hoàn thành, chúng ta có một tài sản dữ liệu (data asset) cực kỳ giá trị tại Staging Layer. Nó là một bản ghi log đầy đủ, có thể kiểm toán, và là nền tảng vững chắc để các quy trình xử lý ở Giai đoạn 2 (Transform & Load to Historical) có thể hoạt động một cách đáng tin cậy.