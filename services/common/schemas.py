from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class TransactionRequest(BaseModel):
    model_config = ConfigDict(
        str_strip_whitespace=True,
        extra="forbid",
    )

    user_id: str = Field(min_length=1)
    amount: Decimal = Field(
        description="Signed amount: positive adds funds, negative withdraws funds."
    )


class Transaction(TransactionRequest):
    transaction_id: str = Field(min_length=1)
    timestamp: datetime


class LogStoreResponse(BaseModel):
    transaction_id: str
    stored: bool


class BalanceResponse(BaseModel):
    user_id: str
    balance: Decimal


class AccountsResponse(BaseModel):
    balances: dict[str, Decimal]


class TransactionResult(BaseModel):
    transaction_id: str
    balance: Decimal


class UserSnapshot(BaseModel):
    user_id: str
    balance: Decimal
    transactions: list[Transaction]


class MetricsResponse(BaseModel):
    logging_service_total_ms: float
    counter_service_total_ms: float
    logging_service_calls: int
    counter_service_calls: int
    logging_service_avg_ms: float
    counter_service_avg_ms: float


class HealthResponse(BaseModel):
    service: str
    status: Literal["ok"]
