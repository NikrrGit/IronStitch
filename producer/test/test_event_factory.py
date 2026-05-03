from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from event_factory import row_to_event

def test_row_to_event():
    row = {
        "order_id": "123",
        "order_item_id": 1,
        "product_id": "product1",
        "seller_id": "seller1",
        "price": 100.00,
        "freight_value": 12.00,
    }

    event = row_to_event(row)

    assert event["order_id"] == "123"
    assert event["price"] == 100.00
    assert "event_id" in event
    assert "event_time" in event


    