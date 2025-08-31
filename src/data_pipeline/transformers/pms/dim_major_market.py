# import logging
# from __future__ import annotations
# from typing import Dict, List, Any, Optional

# from src.data_pipeline.transformers.abstract_transformer import (
#     AbstractTransformer, TransformResult
# )
# from src.data_pipeline.schemas.pms_schemas import DimMajorMarket

# class DimMajorMarket(AbstractTransformer[Dict[str, Any], Dict[str, Any]]):
#     """
#     Tạo ra 1 bảng Dim_Major_Market hoàn chỉnh
#     """
#     def __init__(self) -> None:
#         super().__init__(
#             name = "DimMajorMarketTransformer",
#             output_schema= DimMajorMarket,
#             dedup_keys=("price_list_id",),
#             source= "PMS:Bookings:Major Market" ,
#             scd_config= True, 
#             on_validation_error= "drop"
#         )

    