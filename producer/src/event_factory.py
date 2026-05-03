from datetime import datetime, timezone
from uuid import uuid4

def row_to_event(row): -> dict:
    return  {
            "event_id": str(uuid4()),
            "event_time": datetime.now(timezone.utc).isoformat(),
            "order_id": str(row["order_id"]),
            "order_item_id": int(row["order_item_id"]),
            "product_id": str(row["product_id"]),
            "seller_id": str(row["seller_id"]),
            "price": float(row["price"]),
            "freight_value": float(row["freight_value"]),
    }
    