from abc import ABC, abstractmethod
from typing import List, Dict, Any
import pandas as pd

class AbstractParser(ABC):

    @abstractmethod
    def parse(self,
              data: List[Dict[str,Any]],
              **kwargs) -> pd.DataFrame:
        """
        Phương thức chính để biến đổi dữ liệu thô (list of dicts) thành DataFrame.
        
        Args:
            data: Dữ liệu thô từ Extractor.
            **kwargs: Các tham số bổ sung nếu cần (ví dụ: branch_id).
            
        Returns:
            Một pandas DataFrame đã được làm sạch và cấu trúc.
        """
        pass
    