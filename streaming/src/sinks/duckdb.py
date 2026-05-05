from pathlib import Path

import duckdb


# insert the flattened data to duckdb
def write_to_duckdb(batch_df, batch_id):
    del batch_id
    if batch_df.rdd.isEmpty():
        return

    db_path = Path("data/duckdb/orders.duckdb")
    db_path.parent.mkdir(parents=True, exist_ok=True)

    pandas_df = batch_df.toPandas()
    conn = duckdb.connect(str(db_path))
    try:
        conn.register("incoming_batch", pandas_df)
        conn.execute("CREATE TABLE IF NOT EXISTS orders AS SELECT * FROM incoming_batch LIMIT 0")
        conn.execute("INSERT INTO orders SELECT * FROM incoming_batch")
        conn.unregister("incoming_batch")
    finally:
        conn.close()