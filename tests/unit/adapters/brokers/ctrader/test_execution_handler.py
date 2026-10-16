from app.adapters.brokers.ctrader.execution_handler import map_execution_report


class _MockMessage:
    def __init__(self, fields: dict[int, str | list[str]]) -> None:
        self._fields = fields

    def getFieldValue(self, tag: int) -> str | list[str] | None:
        return self._fields.get(tag)

    def getMessage(self) -> str:
        return "8=FIX.4.4|...raw..."


def test_map_execution_report_extracts_the_documented_tags() -> None:
    message = _MockMessage(
        {
            11: "abc123",
            17: "exec-1",
            150: "2",
            39: "2",
            55: "1",
            32: "1000",
            31: "1.08243",
            6: "1.08200",
            14: "1000",
            151: "0",
            721: "4471123",
            60: "20260101-00:00:00.000",
        }
    )

    event = map_execution_report(message)

    assert event.fix_cl_ord_id == "abc123"
    assert event.exec_id == "exec-1"
    assert event.exec_type == "2"
    assert event.ord_status == "2"
    assert event.symbol == "1"
    assert event.last_qty == "1000"
    assert event.last_price == "1.08243"
    assert event.avg_price == "1.08200"
    assert event.cum_qty == "1000"
    assert event.leaves_qty == "0"
    assert event.position_id == "4471123"
    assert event.order_id is None
    assert event.raw == "8=FIX.4.4|...raw..."


def test_map_execution_report_takes_the_first_element_of_a_repeating_group_field() -> None:
    message = _MockMessage({11: "abc123", 150: "0", 39: "0", 14: "0", 151: "1000", 31: ["1.08000", "1.08010"]})

    event = map_execution_report(message)

    assert event.last_price == "1.08000"


def test_map_execution_report_defaults_missing_optional_fields_to_none() -> None:
    message = _MockMessage({11: "abc123", 150: "0", 39: "0", 14: "0", 151: "1000"})

    event = map_execution_report(message)

    assert event.exec_id is None
    assert event.last_qty is None
    assert event.last_price is None
    assert event.avg_price is None
    assert event.position_id is None
    assert event.text is None


def test_map_execution_report_synthesizes_exec_id_when_tag_17_is_absent() -> None:
    # A real cTrader demo account's ExecutionReports never carry tag 17
    # (ExecID) at all, despite FIX 4.4 marking it mandatory - observed live.
    message = _MockMessage(
        {
            11: "abc123",
            37: "319319136",
            150: "0",
            39: "0",
            14: "0",
            151: "1000",
            60: "20260922-11:05:52.919",
        }
    )

    event = map_execution_report(message)

    assert event.exec_id == "319319136:0:20260922-11:05:52.919"


def test_map_execution_report_synthesized_exec_id_differs_across_the_same_order_s_reports() -> None:
    # The New and Filled reports for one order share OrderID (37) but differ
    # in ExecType/TransactTime - the fallback must not collapse them into a
    # duplicate-key clash.
    new_report = _MockMessage(
        {11: "abc123", 37: "319319136", 150: "0", 39: "0", 14: "0", 151: "1000", 60: "20260922-11:05:52.919"}
    )
    filled_report = _MockMessage(
        {11: "abc123", 37: "319319136", 150: "F", 39: "2", 14: "1000", 151: "0", 60: "20260922-11:05:53.193"}
    )

    assert map_execution_report(new_report).exec_id != map_execution_report(filled_report).exec_id


def test_map_execution_report_synthesized_exec_id_is_stable_for_a_retransmission() -> None:
    # A redelivered/retransmitted copy of the exact same report must produce
    # the exact same fallback id, or the dedup this fallback exists for
    # (uq_executions_account_exec_id) can't do its job.
    message = _MockMessage(
        {11: "abc123", 37: "319319136", 150: "F", 39: "2", 14: "1000", 151: "0", 60: "20260922-11:05:53.193"}
    )

    assert map_execution_report(message).exec_id == map_execution_report(message).exec_id


def test_map_execution_report_extracts_avg_px_when_last_px_is_absent() -> None:
    # A real cTrader demo account's Filled report never carries tag 31
    # (LastPx), only tag 6 (AvgPx) - observed live. last_price and avg_price
    # are tracked independently now (previously conflated into one field,
    # which mispriced avg_fill_price for a partially filled order).
    message = _MockMessage({11: "abc123", 150: "F", 39: "2", 14: "1000", 151: "0", 32: "1000", 6: "1.14639"})

    event = map_execution_report(message)

    assert event.last_price is None
    assert event.avg_price == "1.14639"


def test_map_execution_report_extracts_last_px_and_avg_px_independently_when_both_present() -> None:
    message = _MockMessage({11: "abc123", 150: "F", 39: "2", 14: "1000", 151: "0", 31: "1.08243", 6: "1.08200"})

    event = map_execution_report(message)

    assert event.last_price == "1.08243"
    assert event.avg_price == "1.08200"
