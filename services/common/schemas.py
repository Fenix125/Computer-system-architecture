from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


TransactionStatus = Literal["pending", "applied", "rejected"]


class StrictBaseModel(BaseModel):
    model_config = ConfigDict(
        str_strip_whitespace=True,
        extra="forbid",
    )


class TransactionRequest(StrictBaseModel):
    user_id: str = Field(min_length=1)
    amount: Decimal = Field(
        description="Signed amount: positive adds funds, negative withdraws funds."
    )


class Transaction(TransactionRequest):
    transaction_id: str = Field(min_length=1)
    timestamp: datetime


class StoredTransaction(Transaction):
    status: TransactionStatus
    status_reason: str | None = None


class LogStoreResponse(StrictBaseModel):
    transaction_id: str
    stored: bool


class TransactionStatusUpdateRequest(StrictBaseModel):
    status: TransactionStatus
    status_reason: str | None = None


class QueuedTransactionResponse(StrictBaseModel):
    transaction_id: str
    status: Literal["pending"]
    queued: bool


class BalanceResponse(StrictBaseModel):
    user_id: str
    balance: Decimal


class AccountsResponse(StrictBaseModel):
    balances: dict[str, Decimal]


class UserSnapshot(StrictBaseModel):
    user_id: str
    balance: Decimal
    transactions: list[StoredTransaction]


class MetricsResponse(StrictBaseModel):
    logging_service_total_ms: float
    counter_service_total_ms: float
    kafka_publish_total_ms: float
    logging_service_calls: int
    counter_service_calls: int
    kafka_publish_calls: int
    logging_service_avg_ms: float
    counter_service_avg_ms: float
    kafka_publish_avg_ms: float


class HealthResponse(StrictBaseModel):
    service: str
    status: Literal["ok"]


class ServiceInstance(StrictBaseModel):
    instance_name: str = Field(min_length=1)
    instance_url: str = Field(min_length=1)
