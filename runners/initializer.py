import ipaddress
import json
import logging
import random
import uuid
from datetime import datetime, timedelta

import faker
import pyarrow as pa

from flight_server import FLIGHT_PORT
from runners.writer import run_writer

logger = logging.getLogger()


def create_sample_table(schema=None, num_rows=100, data_profile="analytics"):
    """
    Create a sample table with realistic data.

    Args:
        schema: Optional custom schema. If None, will create schema based on data_profile.
        num_rows: Number of rows to generate.
        data_profile: Type of data to generate ("analytics", "events", "sales", "iot").

    Returns:
        PyArrow Table with sample data
    """
    # Initialize faker for generating realistic data
    fake = faker.Faker()

    if schema is None:
        if data_profile == "analytics":
            # Web analytics data profile
            schema = pa.schema(
                [
                    pa.field("visitor_id", pa.string(), nullable=False),
                    pa.field("session_id", pa.string(), nullable=False),
                    pa.field("timestamp", pa.timestamp("ms"), nullable=False),
                    pa.field("page_url", pa.string(), nullable=False),
                    pa.field("referrer", pa.string()),
                    pa.field("user_agent", pa.string()),
                    pa.field("device_type", pa.string()),
                    pa.field("country", pa.string()),
                    pa.field("city", pa.string()),
                    pa.field("browser", pa.string()),
                    pa.field("os", pa.string()),
                    pa.field("duration_seconds", pa.int32()),
                    pa.field("page_views", pa.int16()),
                    pa.field("conversion", pa.bool_()),
                ]
            )

            # Generate data
            data = []
            for _ in range(num_rows):
                visitor_id = str(uuid.uuid4())
                timestamp = fake.date_time_between(start_date="-30d", end_date="now")

                data.append(
                    {
                        "visitor_id": visitor_id,
                        "session_id": f"{visitor_id}_{int(timestamp.timestamp())}",
                        "timestamp": timestamp,
                        "page_url": fake.uri_path(),
                        "referrer": fake.uri() if random.random() > 0.3 else None,
                        "user_agent": fake.user_agent(),
                        "device_type": random.choice(["desktop", "mobile", "tablet"]),
                        "country": fake.country(),
                        "city": fake.city(),
                        "browser": random.choice(
                            ["Chrome", "Firefox", "Safari", "Edge"]
                        ),
                        "os": random.choice(
                            ["Windows", "MacOS", "Linux", "iOS", "Android"]
                        ),
                        "duration_seconds": random.randint(5, 1800),
                        "page_views": random.randint(1, 20),
                        "conversion": random.random() > 0.9,
                    }
                )

        elif data_profile == "events":
            # Event log data profile
            schema = pa.schema(
                [
                    pa.field("event_id", pa.string(), nullable=False),
                    pa.field("event_type", pa.string(), nullable=False),
                    pa.field("timestamp", pa.timestamp("ms"), nullable=False),
                    pa.field("user_id", pa.string()),
                    pa.field("device_id", pa.string()),
                    pa.field("ip_address", pa.string()),
                    pa.field("severity", pa.string()),
                    pa.field("component", pa.string()),
                    pa.field("message", pa.string()),
                    pa.field("properties", pa.string()),
                    pa.field("duration_ms", pa.int64()),
                    pa.field("status_code", pa.int16()),
                ]
            )

            # Generate data
            data = []
            event_types = [
                "page_view",
                "click",
                "form_submit",
                "api_call",
                "error",
                "login",
                "logout",
            ]
            components = [
                "frontend",
                "backend",
                "database",
                "auth",
                "api",
                "cache",
                "scheduler",
            ]
            severities = ["debug", "info", "warning", "error", "critical"]

            for _ in range(num_rows):
                event_type = random.choice(event_types)
                timestamp = fake.date_time_between(start_date="-7d", end_date="now")
                duration = (
                    random.randint(1, 5000)
                    if event_type in ["api_call", "form_submit"]
                    else None
                )
                status = (
                    random.choice([200, 201, 400, 404, 500])
                    if event_type == "api_call"
                    else None
                )

                data.append(
                    {
                        "event_id": str(uuid.uuid4()),
                        "event_type": event_type,
                        "timestamp": timestamp,
                        "user_id": str(uuid.uuid4()) if random.random() > 0.2 else None,
                        "device_id": (
                            fake.mac_address() if random.random() > 0.3 else None
                        ),
                        "ip_address": str(
                            ipaddress.IPv4Address(random.randint(0, 2 ** 32 - 1))
                        ),
                        "severity": random.choice(severities),
                        "component": random.choice(components),
                        "message": fake.sentence(),
                        "properties": (
                            json.dumps({"key1": fake.word(), "key2": fake.word()})
                            if random.random() > 0.5
                            else None
                        ),
                        "duration_ms": duration,
                        "status_code": status,
                    }
                )

        elif data_profile == "sales":
            # Sales data profile
            schema = pa.schema(
                [
                    pa.field("order_id", pa.string(), nullable=False),
                    pa.field("customer_id", pa.string(), nullable=False),
                    pa.field("transaction_date", pa.timestamp("ms"), nullable=False),
                    pa.field("product_id", pa.string(), nullable=False),
                    pa.field("product_name", pa.string(), nullable=False),
                    pa.field("category", pa.string()),
                    pa.field("quantity", pa.int16(), nullable=False),
                    pa.field("unit_price", pa.float64(), nullable=False),
                    pa.field("total_amount", pa.float64(), nullable=False),
                    pa.field("payment_method", pa.string()),
                    pa.field("store_id", pa.string()),
                    pa.field("salesperson", pa.string()),
                    pa.field("promotion_code", pa.string()),
                    pa.field("is_returned", pa.bool_()),
                ]
            )

            # Generate data
            data = []
            products = [
                {
                    "id": "P001",
                    "name": "Laptop",
                    "category": "Electronics",
                    "price": 1299.99,
                },
                {
                    "id": "P002",
                    "name": "Smartphone",
                    "category": "Electronics",
                    "price": 899.99,
                },
                {
                    "id": "P003",
                    "name": "Coffee Maker",
                    "category": "Appliances",
                    "price": 79.99,
                },
                {
                    "id": "P004",
                    "name": "Running Shoes",
                    "category": "Footwear",
                    "price": 129.99,
                },
                {
                    "id": "P005",
                    "name": "Office Chair",
                    "category": "Furniture",
                    "price": 249.99,
                },
                {
                    "id": "P006",
                    "name": "Headphones",
                    "category": "Electronics",
                    "price": 199.99,
                },
                {
                    "id": "P007",
                    "name": "Backpack",
                    "category": "Accessories",
                    "price": 59.99,
                },
                {
                    "id": "P008",
                    "name": "Desk Lamp",
                    "category": "Lighting",
                    "price": 39.99,
                },
            ]

            payment_methods = [
                "Credit Card",
                "Debit Card",
                "PayPal",
                "Cash",
                "Bank Transfer",
            ]
            store_ids = ["S001", "S002", "S003", "S004", "S005"]

            # Generate 20 customers
            customers = [str(uuid.uuid4()) for _ in range(20)]

            for _ in range(num_rows):
                product = random.choice(products)
                quantity = random.randint(1, 5)
                total = round(product["price"] * quantity, 2)
                customer = random.choice(customers)
                transaction_date = fake.date_time_between(
                    start_date="-90d", end_date="now"
                )

                data.append(
                    {
                        "order_id": str(uuid.uuid4()),
                        "customer_id": customer,
                        "transaction_date": transaction_date,
                        "product_id": product["id"],
                        "product_name": product["name"],
                        "category": product["category"],
                        "quantity": quantity,
                        "unit_price": product["price"],
                        "total_amount": total,
                        "payment_method": random.choice(payment_methods),
                        "store_id": random.choice(store_ids),
                        "salesperson": fake.name(),
                        "promotion_code": (
                            f"PROMO{random.randint(10, 99)}"
                            if random.random() > 0.7
                            else None
                        ),
                        "is_returned": random.random() < 0.05,
                    }
                )

        elif data_profile == "iot":
            # IoT sensor data profile
            schema = pa.schema(
                [
                    pa.field("reading_id", pa.string(), nullable=False),
                    pa.field("device_id", pa.string(), nullable=False),
                    pa.field("sensor_type", pa.string(), nullable=False),
                    pa.field("timestamp", pa.timestamp("ms"), nullable=False),
                    pa.field("value", pa.float64(), nullable=False),
                    pa.field("unit", pa.string()),
                    pa.field("latitude", pa.float64()),
                    pa.field("longitude", pa.float64()),
                    pa.field("battery_level", pa.float32()),
                    pa.field("signal_strength", pa.int8()),
                    pa.field("alert_triggered", pa.bool_()),
                    pa.field("firmware_version", pa.string()),
                ]
            )

            # Generate data
            data = []

            sensor_types = [
                {"type": "temperature", "unit": "celsius", "min": -10, "max": 50},
                {"type": "humidity", "unit": "percent", "min": 0, "max": 100},
                {"type": "pressure", "unit": "hPa", "min": 900, "max": 1100},
                {"type": "air_quality", "unit": "ppm", "min": 0, "max": 500},
                {"type": "light", "unit": "lux", "min": 0, "max": 10000},
                {"type": "noise", "unit": "dB", "min": 20, "max": 120},
            ]

            # Generate 30 devices
            devices = [
                f"IOT-{fake.lexify('???')}-{fake.numerify('####')}" for _ in range(30)
            ]
            firmware_versions = ["v1.0.0", "v1.1.2", "v1.2.0", "v2.0.1", "v2.1.0"]

            for _ in range(num_rows):
                device_id = random.choice(devices)
                sensor = random.choice(sensor_types)
                timestamp = fake.date_time_between(start_date="-3d", end_date="now")
                value = random.uniform(sensor["min"], sensor["max"])
                # Some sensors trigger alerts on certain thresholds
                alert = False
                if sensor["type"] == "temperature" and (value > 40 or value < 0):
                    alert = True
                elif sensor["type"] == "air_quality" and value > 300:
                    alert = True

                data.append(
                    {
                        "reading_id": str(uuid.uuid4()),
                        "device_id": device_id,
                        "sensor_type": sensor["type"],
                        "timestamp": timestamp,
                        "value": round(value, 2),
                        "unit": sensor["unit"],
                        "latitude": float(fake.latitude()),
                        "longitude": float(fake.longitude()),
                        "battery_level": round(random.uniform(0, 100), 1),
                        "signal_strength": random.randint(-120, -30),
                        "alert_triggered": alert,
                        "firmware_version": random.choice(firmware_versions),
                    }
                )
        else:
            # Default simple schema if no profile matches
            schema = pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=False),
                    pa.field("value", pa.string(), nullable=False),
                    pa.field("timestamp", pa.timestamp("ms"), nullable=False),
                    pa.field("is_active", pa.bool_()),
                ]
            )

            data = []
            for i in range(num_rows):
                data.append(
                    {
                        "id": i + 1,
                        "value": f"sample_value_{i + 1}",
                        "timestamp": datetime.now()
                                     - timedelta(hours=random.randint(0, 24 * 7)),
                        "is_active": random.choice([True, False]),
                    }
                )

    # Use the provided schema if one was passed in
    return pa.Table.from_pylist(data, schema=schema)


def initialize_data(table_name: str, port: int):
    try:

        # Create initial tables with different data profiles if they don't exist
        logger.info(
            f"Creating sample table {table_name} with analytics data profile"
        )
        sample_table = create_sample_table(num_rows=200, data_profile="analytics")
        run_writer(table_name, port, sample_table)
    except Exception as e:
        logger.error(f"Error writing to server: {e}")
        raise


if __name__ == "__main__":
    table_name = "sample_table"
    initialize_data(table_name, FLIGHT_PORT)
