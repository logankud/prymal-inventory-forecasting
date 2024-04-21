from pydantic import BaseModel, Field
from datetime import date
from typing import Optional

class ShopifyDemandForecast(BaseModel):
    sku: int
    sku_name: str
    forecast: str
    lower_bound: float
    upper_bound: float
    last_7_actual: int
    last_90_actual: int
    product_type: str
    product_category: str
    inventory_on_hand: int
    forecast_90_days: int
    production_next_90_days: int
    forecast_120_days: int
    production_next_120_days: int
    forecast_150_days: int
    production_next_150_days: int
    days_of_stock_on_hand: int
    forecasted_stockout_date: date