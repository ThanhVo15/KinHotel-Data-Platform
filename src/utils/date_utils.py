# E:\Job\Kin-Hotel\DE\KinHotelAutoDashboard\src\data_pipeline\processors\utils\date_utils.py
import pandas as pd
from datetime import date

def create_dim_date(start_date: date, end_date: date) -> pd.DataFrame:
    """
    Tạo bảng DimDate chuẩn, đầy đủ và tối ưu từ ngày bắt đầu đến ngày kết thúc.
    Phiên bản này được tối ưu để sử dụng các thuộc tính vector của Pandas,
    tránh dùng .apply() để tăng hiệu suất.
    """
    if start_date > end_date:
        raise ValueError("Start date cannot be after end date.")
        
    # Tạo một Series các đối tượng Timestamp của Pandas
    date_series = pd.to_datetime(pd.date_range(start_date, end_date, freq='D'))
    df = pd.DataFrame({'timestamp': date_series})
    
    # === TÍNH TOÁN TẤT CẢ CÁC THUỘC TÍNH TỪ PANDAS TIMESTAMP ===
    df['date_key'] = df['timestamp'].dt.strftime('%Y%m%d').astype(int)
    df['full_date'] = df['timestamp'].dt.date # Chuyển sang date object ở cuối
    
    # Year Attributes
    df['year'] = df['timestamp'].dt.year
    
    # Quarter Attributes
    df['quarter'] = df['timestamp'].dt.quarter
    df['quarter_name'] = df['year'].astype(str) + '-Q' + df['quarter'].astype(str)
    
    # Month Attributes
    df['month'] = df['timestamp'].dt.month
    df['month_name'] = df['timestamp'].dt.month_name()
    df['month_name_short'] = df['timestamp'].dt.strftime('%b')
    df['month_year'] = df['timestamp'].dt.strftime('%Y-%m')

    # Week Attributes
    isocal = df['timestamp'].dt.isocalendar()
    df['week_of_year'] = isocal.week.astype(int)
    
    # Day Attributes
    df['day_of_month'] = df['timestamp'].dt.day
    df['day_of_year'] = df['timestamp'].dt.dayofyear
    df['day_of_week'] = df['timestamp'].dt.dayofweek + 1 # Monday=1, Sunday=7
    df['day_name'] = df['timestamp'].dt.day_name()
    df['day_name_short'] = df['timestamp'].dt.strftime('%a')
    
    # Business Attributes
    df['is_weekend'] = df['day_of_week'].isin([6, 7])
    df['is_weekday'] = ~df['is_weekend']
    
    # Period Flags
    df['is_month_start'] = df['timestamp'].dt.is_month_start
    df['is_month_end'] = df['timestamp'].dt.is_month_end
    df['is_quarter_start'] = df['timestamp'].dt.is_quarter_start
    df['is_quarter_end'] = df['timestamp'].dt.is_quarter_end
    df['is_year_start'] = df['timestamp'].dt.is_year_start
    df['is_year_end'] = df['timestamp'].dt.is_year_end
    
    # Sắp xếp lại các cột cho đẹp
    final_cols = [
        'date_key', 'full_date', 'year', 'quarter', 'quarter_name',
        'month', 'month_name', 'month_name_short', 'month_year',
        'week_of_year', 'day_of_month', 'day_of_year', 'day_of_week',
        'day_name', 'day_name_short', 'is_weekend', 'is_weekday',
        'is_month_start', 'is_month_end', 'is_quarter_start', 'is_quarter_end',
        'is_year_start', 'is_year_end'
    ]
    
    return df[final_cols]