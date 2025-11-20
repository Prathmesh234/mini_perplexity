import os
import json
from pathlib import Path
from typing import Dict, Any, Optional

from dotenv import load_dotenv
from azure.servicebus import (
    ServiceBusClient,
    ServiceBusReceiver,
    ServiceBusReceivedMessage,
    ServiceBusMessage,
)

from schema import EmbeddingChunk


def load_env_config() -> Dict[str, str]:
    """Load environment variables from .env if present."""
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)

    config = {
        "service_bus_conn_str": os.getenv("SERVICE_BUS_CONN_STR", ""),
        "topic_name_ingestion": os.getenv("TOPIC_NAME_INGESTION", "ingestion"),
        "topic_name_output": os.getenv("TOPIC_NAME_OUTPUT", ""),
    }

    if not config["service_bus_conn_str"]:
        raise RuntimeError("SERVICE_BUS_CONN_STR env var is required")

    return config


def _deserialize_message_body(message: ServiceBusReceivedMessage) -> Optional[Dict[str, Any]]:
    """Best-effort conversion of a Service Bus message into a dict."""
    try:
        body_bytes = b"".join(bytes(segment) for segment in message.body)
        if not body_bytes:
            return {}
        return json.loads(body_bytes.decode("utf-8"))
    except (ValueError, TypeError, json.JSONDecodeError):
        try:
            return json.loads(str(message))
        except json.JSONDecodeError:
            return None


def get_ingestion_message(
    subscription_name: str = "ingestion-sub",
    max_wait_time: int = 5,
) -> Optional[EmbeddingChunk]:
    """Retrieve a single message from the ingestion topic."""
    config = load_env_config()
    servicebus_client = ServiceBusClient.from_connection_string(config["service_bus_conn_str"])

    try:
        receiver: ServiceBusReceiver = servicebus_client.get_subscription_receiver(
            topic_name=config["topic_name_ingestion"],
            subscription_name=subscription_name,
            max_wait_time=max_wait_time,
        )

        with receiver:
            received_msgs = receiver.receive_messages(
                max_message_count=1,
                max_wait_time=max_wait_time,
            )

            if not received_msgs:
                return None

            msg = received_msgs[0]
            payload = _deserialize_message_body(msg)
            if payload is None:
                receiver.dead_letter_message(msg, reason="Invalid JSON")
                return None

            try:
                chunk = EmbeddingChunk.model_validate(payload)
                receiver.complete_message(msg)
                return chunk
            except Exception:
                receiver.dead_letter_message(msg, reason="Schema validation failed")
                return None

    finally:
        servicebus_client.close()
    return None


def publish_chunk(
    chunk_payload: EmbeddingChunk,
    topic_name: Optional[str] = None,
) -> bool:
    """Publish an embedding chunk to the downstream topic (topic name can be provided later)."""
    config = load_env_config()
    target_topic = topic_name or config["topic_name_output"]

    if not target_topic:
        print("No output topic configured; skipping publish.")
        return False

    servicebus_client = ServiceBusClient.from_connection_string(config["service_bus_conn_str"])

    try:
        message = ServiceBusMessage(chunk_payload.model_dump_json())
        with servicebus_client.get_topic_sender(topic_name=target_topic) as sender:
            sender.send_messages(message)

        print(f"Published chunk to topic '{target_topic}'")
        return True

    except Exception as exc:
        print(f"Failed to publish chunk: {exc}")
        return False

    finally:
        servicebus_client.close()
