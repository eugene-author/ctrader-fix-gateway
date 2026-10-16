"""Maps rejection messages to OrderFailed.

35=9 OrderCancelReject and 35=j BusinessMessageReject both fail the order:
rejections arrive as 35=j, not as an ExecutionReport, so both paths must
fail the order.
"""

from ctrader_fix import ResponseMessage

from app.adapters.brokers.ctrader.fields import first
from app.domain.events import OrderFailed


def map_business_message_reject(message: ResponseMessage) -> OrderFailed:
    """Map a 35=j BusinessMessageReject to OrderFailed."""
    ref_id = first(message.getFieldValue(379))  # BusinessRejectRefID
    text = first(message.getFieldValue(58)) or "Business message rejected"
    return OrderFailed(fix_cl_ord_id=ref_id, reason_code="BUSINESS_REJECT", message=text, retryable=False)


def map_order_cancel_reject(message: ResponseMessage) -> OrderFailed:
    """Map a 35=9 OrderCancelReject to OrderFailed."""
    cl_ord_id = first(message.getFieldValue(11))
    text = first(message.getFieldValue(58)) or "Order cancel/replace rejected"
    return OrderFailed(fix_cl_ord_id=cl_ord_id, reason_code="CANCEL_REJECT", message=text, retryable=False)
