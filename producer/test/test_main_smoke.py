from pathlib import Path
import runpy
import sys
import types


def test_main_smoke_runs_and_produces(monkeypatch):
    producer_dir = Path(__file__).resolve().parents[1]
    src_dir = producer_dir / "src"
    main_path = src_dir / "main.py"

    if str(producer_dir) not in sys.path:
        sys.path.insert(0, str(producer_dir))

    produced_messages = []
    polled = {"count": 0}
    flushed = {"count": 0}

    class FakeProducer:
        def __init__(self, conf):
            self.conf = conf

        def produce(self, **kwargs):
            produced_messages.append(kwargs)

        def poll(self, _timeout):
            polled["count"] += 1

        def flush(self):
            flushed["count"] += 1

    class FakeSchemaRegistryClient:
        def __init__(self, conf):
            self.conf = conf

    class FakeAvroSerializer:
        def __init__(self, _client, _schema_str):
            pass

        def __call__(self, value, _ctx):
            return value

    class FakeStringSerializer:
        def __init__(self, _encoding):
            pass

        def __call__(self, value):
            return value

    class FakeSerializationContext:
        def __init__(self, topic, field):
            self.topic = topic
            self.field = field

    fake_message_field = types.SimpleNamespace(VALUE="value")

    fake_ck = types.ModuleType("confluent_kafka")
    fake_ck.Producer = FakeProducer

    fake_schema_registry = types.ModuleType("confluent_kafka.schema_registry")
    fake_schema_registry.SchemaRegistryClient = FakeSchemaRegistryClient

    fake_schema_registry_avro = types.ModuleType("confluent_kafka.schema_registry.avro")
    fake_schema_registry_avro.AvroSerializer = FakeAvroSerializer

    fake_serialization = types.ModuleType("confluent_kafka.serialization")
    fake_serialization.MessageField = fake_message_field
    fake_serialization.SerializationContext = FakeSerializationContext
    fake_serialization.StringSerializer = FakeStringSerializer

    monkeypatch.setitem(sys.modules, "confluent_kafka", fake_ck)
    monkeypatch.setitem(sys.modules, "confluent_kafka.schema_registry", fake_schema_registry)
    monkeypatch.setitem(sys.modules, "confluent_kafka.schema_registry.avro", fake_schema_registry_avro)
    monkeypatch.setitem(sys.modules, "confluent_kafka.serialization", fake_serialization)

    class FakeChunk:
        def iterrows(self):
            return [
                (
                    0,
                    {
                        "order_id": "order_1",
                        "order_item_id": 1,
                        "product_id": "prod_1",
                        "seller_id": "seller_1",
                        "price": 10.5,
                        "freight_value": 2.0,
                    },
                )
            ]

    fake_pd = types.ModuleType("pandas")
    fake_pd.read_csv = lambda *_args, **_kwargs: [FakeChunk()]
    monkeypatch.setitem(sys.modules, "pandas", fake_pd)

    monkeypatch.setenv("BROKER", "localhost:19092")
    monkeypatch.setenv("SCHEMA_REGISTRY_URL", "http://localhost:18081")
    monkeypatch.setenv("topic", "orders.v1")
    monkeypatch.setenv("csv_path", "dummy.csv")

    runpy.run_path(str(main_path), run_name="__main__")

    assert len(produced_messages) == 1
    assert produced_messages[0]["topic"] == "orders.v1"
    assert produced_messages[0]["key"] == "order_1"
    assert produced_messages[0]["value"]["order_id"] == "order_1"
    assert polled["count"] == 1
    assert flushed["count"] == 1
