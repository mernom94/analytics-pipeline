class AnalyticsPipelineError(Exception):
    """Base exception for the analytics pipeline."""


class EventValidationError(AnalyticsPipelineError):
    """Raised when an event fails validation."""

    def __init__(self, message: str, field: str | None = None) -> None:
        super().__init__(message)
        self.field = field


class MerchantNotFoundError(AnalyticsPipelineError):
    """Raised when a merchant_id does not exist."""

    def __init__(self, merchant_id: str) -> None:
        super().__init__(f"Merchant not found: {merchant_id}")
        self.merchant_id = merchant_id


class DuplicateEventError(AnalyticsPipelineError):
    """Raised when an idempotency_key has already been used."""

    def __init__(self, idempotency_key: str, existing_event_id: str) -> None:
        super().__init__(f"Duplicate event: idempotency_key={idempotency_key}")
        self.idempotency_key = idempotency_key
        self.existing_event_id = existing_event_id


class RollupWorkerError(AnalyticsPipelineError):
    """Raised by the rollup worker on unrecoverable errors."""


class QueryRoutingError(AnalyticsPipelineError):
    """Raised when query parameters cannot be routed to a rollup table."""
