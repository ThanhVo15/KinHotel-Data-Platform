## Quy trình Vận hành - Giai đoạn 4: Tải dữ liệu & Báo cáo (Loading & Reporting)

### 1\. Tổng quan

Đây là giai đoạn cuối cùng, khép lại chu trình pipeline dữ liệu hàng ngày. Giai đoạn này thực hiện hai nhiệm vụ quan trọng:

1.  **Tải dữ liệu (Loading):** Sao lưu an toàn toàn bộ các tài sản dữ liệu đã được xử lý (`Staging`, `Historical`, `DWH`) lên một kho lưu trữ trung tâm trên Google Drive.
2.  **Báo cáo (Reporting):** Tự động gửi một email báo cáo chi tiết về kết quả của toàn bộ pipeline, thông báo cho các bên liên quan về trạng thái (thành công/thất bại), thời gian chạy và các số liệu quan trọng.

Giai đoạn này được thực thi bởi hai thành phần chính: `GoogleDriveLoader` và `EmailNotifier`.

-----

### 2\. Mục tiêu chính 🎯

  * **Sao lưu & An toàn Dữ liệu:** Đảm bảo một bản sao của tất cả các lớp dữ liệu được lưu trữ an toàn trên đám mây, phòng chống rủi ro và mất mát dữ liệu.
  * **Giám sát Tự động (Automated Monitoring):** Cung cấp phản hồi ngay lập tức về tình trạng của pipeline mỗi ngày. Người vận hành không cần phải kiểm tra log thủ công để biết pipeline có chạy thành công hay không.
  * **Hoàn thiện Chu trình & Thông báo:** Đánh dấu sự kết thúc của một lượt chạy bằng việc lưu trữ tài sản và thông báo kết quả một cách minh bạch.

-----

### 3\. Kiến trúc Luồng Dữ liệu 🏗️

Sau khi Giai đoạn 1 (Staging) và 2 (Transformation) hoàn thành, Giai đoạn 4 được kích hoạt bởi file `main.py`.

#### 3.1. Tải dữ liệu lên Google Drive (thực thi bởi `GoogleDriveLoader`)

  * **Xác thực:** Sử dụng Service Account của Google Cloud để đăng nhập an toàn và tự động.
  * **Đồng bộ hóa:** `GoogleDriveLoader` sẽ quét các thư mục cục bộ (`./data/staging`, `./data/historical`, `./data/dwh`) và sao chép cấu trúc đó lên thư mục gốc đã được chỉ định trên Google Drive.
  * **Cập nhật thông minh:** Nó sẽ kiểm tra xem file đã tồn tại hay chưa. Nếu chưa, nó sẽ tải lên file mới. Nếu đã có, nó sẽ cập nhật nội dung của file đó.
  * **Hỗ trợ Shared Drive:** Loader được thiết kế để tương thích hoàn toàn với Google Shared Drive.

#### 3.2. Thu thập và Tổng hợp Báo cáo (thực thi bởi `main.py`)

  * Trong suốt quá trình chạy, `main.py` hoạt động như một "người giám sát", thu thập các đối tượng kết quả (`ExtractionResult`, `ProcessingResult`) từ mỗi bước.
  * Sau khi tất cả các giai đoạn kết thúc, nó tổng hợp toàn bộ thông tin này vào một đối tượng báo cáo duy nhất, chứa các số liệu như:
      * Trạng thái tổng thể (SUCCESS/FAILED).
      * Tổng thời gian chạy.
      * Số lượng bản ghi mới ở lớp Staging cho từng chi nhánh.
      * Số lượng bản ghi đầu vào/đầu ra ở lớp Historical.
      * Số lượng bản ghi ở các bảng DWH.
      * Thông tin chi tiết lỗi nếu pipeline thất bại.

#### 3.3. Gửi Email Báo cáo (thực thi bởi `EmailNotifier`)

  * `EmailNotifier` nhận đối tượng báo cáo tổng hợp từ `main.py`.
  * Nó kết nối đến máy chủ SMTP của Gmail bằng các thông tin đăng nhập đã được cấu hình.
  * Nó định dạng báo cáo thành một email HTML sạch sẽ, dễ đọc với các màu sắc để làm nổi bật trạng thái thành công (xanh) hoặc thất bại (đỏ).
  * **Quan trọng:** Bước này được đặt trong khối `finally` của `main.py`, đảm bảo rằng một email báo cáo **luôn được gửi đi**, dù cho pipeline thành công hay thất bại giữa chừng.

-----

### 4\. Cấu hình Bắt buộc

Để Giai đoạn 4 hoạt động, các biến môi trường sau trong file `.env` phải được cấu hình chính xác.

  * **Cho Google Drive:**

      * `GDRIVE_FOLDER_ID`: ID của thư mục gốc trên Google Drive.
      * `GDRIVE_SA_PATH`: Đường dẫn đến file key JSON của Service Account.
      * **Quyền truy cập:** Email của Service Account phải được **Share** quyền **Editor** vào thư mục gốc trên.

  * **Cho Email:**

      * `EMAIL_SENDER`: Địa chỉ email Gmail của người gửi (ví dụ: `your.robot.email@gmail.com`).
      * `EMAIL_PASSWORD`: **Mật khẩu ứng dụng (App Password)** của email người gửi. Do chính sách bảo mật của Google, bạn không thể dùng mật khẩu thông thường. Hãy vào cài đặt tài khoản Google của bạn \> Security \> 2-Step Verification \> App passwords để tạo một mật khẩu mới.
      * `EMAIL_RECIPIENT`: Địa chỉ email của người nhận báo cáo.

-----

### 5\. Mẫu Email Kết quả

Đây là ví dụ về email báo cáo bạn sẽ nhận được.

#### Khi chạy thành công:

> **Pipeline Data Report - 2025-09-11**
>
> **Overall Status:** \<span style="color: green; font-weight: bold;"\>SUCCESS\</span\>
> **Total Execution Time:** 123.45 seconds
>
> -----
>
> **Phase 1: Staging Layer**
>
> | Branch ID | New Records | Status |
> | :--- | :--- | :--- |
> | 1 | 386 | success |
> | 2 | 531 | success |
>
> -----
>
> **Phase 4: Google Drive Upload**
>
> | Data Layer | Files Uploaded | Files Updated | Status |
> | :--- | :--- | :--- | :--- |
> | Staging | 9 | 0 | success |
> | Historical | 10 | 0 | success |
> | DWH | 3 | 0 | success |

#### Khi chạy thất bại:

> **Pipeline Data Report - 2025-09-11**
>
> **Overall Status:** \<span style="color: red; font-weight: bold;"\>FAILED\</span\>
> **Total Execution Time:** 45.67 seconds
>
> -----
>
> **Error Details**
>
> ```
> Traceback (most recent call last):
>   File "main.py", line 123, in main
>     if not dwh_result.is_success: raise Exception("DWHProcessor FAILED.")
> Exception: DWHProcessor FAILED.
> ```
>
> -----
>
> **Phase 2.2: Data Warehouse Layer**
>
> | Metric | Value |
> | :--- | :--- |
> | Status | \<span style="color: red; font-weight: bold;"\>error\</span\> |
> | Error | Some detailed error message... |

-----

### 6\. Kết quả cuối cùng

Sau khi Giai đoạn 4 hoàn thành, pipeline của bạn đã là một hệ thống tự động end-to-end hoàn chỉnh. Dữ liệu không chỉ được xử lý và mô hình hóa mà còn được sao lưu an toàn và trạng thái của toàn bộ quá trình được thông báo một cách minh bạch đến người vận hành.