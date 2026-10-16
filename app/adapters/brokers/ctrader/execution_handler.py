"""Maps ExecutionReport -> ExecutionReportReceived."""

from ctrader_fix import ResponseMessage

from app.adapters.brokers.ctrader.fields import first
from app.domain.events import ExecutionReportReceived


def map_execution_report(message: ResponseMessage) -> ExecutionReportReceived:
    """Map ResponseMessage to an ExecutionReportReceived event."""
    return ExecutionReportReceived(
        fix_cl_ord_id=first(message.getFieldValue(11)) or "",
        exec_id=first(message.getFieldValue(17)) or _synthesize_exec_id(message),
        exec_type=first(message.getFieldValue(150)) or "",
        ord_status=first(message.getFieldValue(39)) or "",
        symbol=first(message.getFieldValue(55)),
        last_qty=first(message.getFieldValue(32)),
        last_price=first(message.getFieldValue(31)),
        avg_price=first(message.getFieldValue(6)),
        cum_qty=first(message.getFieldValue(14)) or "0",
        leaves_qty=first(message.getFieldValue(151)) or "0",
        position_id=first(message.getFieldValue(721)),
        text=first(message.getFieldValue(58)),
        transact_time=first(message.getFieldValue(60)) or "",
        raw=message.getMessage(),
    )


def _synthesize_exec_id(message: ResponseMessage) -> str | None:
    """Fall back when tag 17 (ExecID) is absent.

    OrderID (37) + ExecType (150) + TransactTime (60) is unique per execution
    and identical on a retransmission of the same report.
    """
    order_id = first(message.getFieldValue(37))
    exec_type = first(message.getFieldValue(150))
    transact_time = first(message.getFieldValue(60))
    if order_id is None or exec_type is None or transact_time is None:
        return None
    return f"{order_id}:{exec_type}:{transact_time}"
