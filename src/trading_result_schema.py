from pydantic import BaseModel, Field, computed_field, ConfigDict
from datetime import date, datetime


class TradingResultModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    exchange_product_id: str
    exchange_product_name: str
    delivery_basis_id: str
    delivery_basis_name: str
    delivery_type_id: str
    volume: float
    total: float
    count: int
    date: date
    created_on: datetime
    updated_on: datetime

    # @computed_field
    # @property
    # def oil_id(self) -> str:
    #     return self.exchange_product_id[:4]

    # @computed_field
    # @property
    # def delivery_basis_id(self) -> str:
    #     return self.exchange_product_id[4:7]

    # @computed_field
    # @property
    # def delivery_type_id(self) -> str:
    #     return self.exchange_product_id[-1]
