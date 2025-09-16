## Quy trình Xử lý Dữ liệu - Giai đoạn 3: Lớp Kho Dữ liệu (Data Warehouse Layer)

### 1\. Tổng quan

Đây là giai đoạn thứ 3 của pipeline, nơi dữ liệu lịch sử từ **Historical Layer** được biến đổi thành một mô hình dữ liệu tối ưu cho việc phân tích, gọi là **Mô hình Ngôi sao (Star Schema)**.

Sản phẩm của giai đoạn này là các bảng dữ liệu sạch sẽ, dễ hiểu, sẵn sàng để kết nối với các công cụ BI (Business Intelligence) như Power BI, Tableau để tạo báo cáo và dashboard.

-----

### 2\. Mục tiêu chính 🎯

  * **Tối ưu cho Phân tích:** Chia tách dữ liệu thành các bảng **Sự kiện (Facts)** và **Chiều (Dimensions)**. Mô hình này giúp các truy vấn tổng hợp (SUM, COUNT, AVG) chạy cực kỳ nhanh.
  * **"Nguồn Chân lý Duy nhất" (Single Source of Truth):** Lớp DWH là nơi cuối cùng, sạch sẽ và đáng tin cậy nhất mà người dùng cuối (Data Analyst, Business User) sẽ tương tác.
  * **Đảm bảo Chất lượng Dữ liệu:** Đây là "cổng kiểm soát chất lượng" cuối cùng, nơi dữ liệu được validate chặt chẽ với các schema Pydantic đã định nghĩa để đảm bảo tính đúng đắn và nhất quán.

-----

### 3\. Kiến trúc Luồng Dữ liệu 🏗️

Quy trình này được thực thi bởi `DWHBookingProcessor`, bao gồm các bước:

#### 3.1. Đọc Trạng thái Mới nhất

Processor chỉ đọc file `_combined_booking_history.parquet` từ snapshot mới nhất của lớp Historical và lọc ra những dòng đang có hiệu lực (`is_current = True`). Điều này đảm bảo DWH luôn phản ánh trạng thái mới nhất của doanh nghiệp.

#### 3.2. Tạo các Bảng Chiều (Dimension Tables)

Từ DataFrame đã lọc, processor sẽ tạo ra các bảng chiều:

  * **`DimDate`:** Một bảng lịch chi tiết được tạo ra dựa trên khoảng thời gian từ ngày booking sớm nhất đến ngày checkout muộn nhất.
  * **`DimMajorMarket`:** Bảng chứa các thông tin về thị trường/bảng giá, được tạo bằng cách lấy các giá trị `pricelist` duy nhất.
  * *(Các Dim khác trong tương lai)*: Tương tự, các bảng như `DimRoom`, `DimCustomer` sẽ được tạo ra.

#### 3.3. Tạo Bảng Sự kiện (Fact Table)

Sau khi có các bảng chiều, processor sẽ tạo ra bảng `FactBooking`:

  * Nó lấy các cột số liệu (measures) như `price`, `booking_days`, `num_adult` và các ID từ DataFrame chính.
  * Quan trọng nhất, nó sẽ thay thế các ID nghiệp vụ (ví dụ: `pricelist_id`) bằng các **khóa thay thế (surrogate keys)** từ các bảng chiều tương ứng (ví dụ: `major_market_key`).

#### 3.4. Ghi Dữ liệu DWH

Mỗi bảng Fact và Dim được tạo ra sẽ được **ghi đè (overwrite)** vào một thư mục riêng trong lớp DWH. Việc ghi đè đảm bảo rằng DWH luôn chứa dữ liệu mới nhất, không chứa lịch sử (lịch sử đã có ở lớp Historical).

-----

### 4\. Cấu trúc Thư mục DWH

```
{DWH_DIR}/
├── /dim_date/
│   └── dim_date.parquet
├── /dim_major_market/
│   └── dim_major_market.parquet
└── /fact_booking/
    └── fact_booking.parquet
```

-----

### 5\. Ví dụ Minh họa

Tiếp nối ví dụ, ta sẽ xem DWH được tạo ra từ snapshot của **Ngày 3 (2025-09-10)**.

  * **Input:** Snapshot `historical/2025/09/10/_combined_booking_history.parquet`, lọc `is_current = True`.
    | booking\_line\_id | price | ... | pricelist\_id |
    | :--- | :--- | :--- | :--- |
    | 101 | 500 | ... | 1 |
    | 103 | 1200 | ... | 2 |
    | 102 | 950 | ... | 1 |

  * **Kết quả Lớp Data Warehouse:**

**File:** `dwh/dim_major_market/dim_major_market.parquet`
| major\_market\_key | pricelist\_id | name |
| :--- | :--- | :--- |
| 1 | 1 | Retail Price |
| 2 | 2 | Corporate Deal |

**File:** `dwh/fact_booking/fact_booking.parquet`
| booking\_line\_id | major\_market\_key | price |
| :--- | :--- | :--- |
| 101 | 1 | 500 |
| 103 | 2 | 1200 |
| 102 | 1 | **950** |

-----

### 6\. Kết quả cuối cùng

Giai đoạn 3 tạo ra một kho dữ liệu có cấu trúc, đã được xác thực và tối ưu hóa. Các nhà phân tích dữ liệu có thể kết nối trực tiếp vào các bảng này để xây dựng báo cáo và dashboard một cách nhanh chóng, chính xác và hiệu quả.