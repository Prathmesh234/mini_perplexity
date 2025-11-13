import os
import json
import time
from pathlib import Path
from typing import Iterator
from dotenv import load_dotenv
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from tqdm import tqdm
from chunk_index import Chunk


def send_chunks_to_service_bus(
    chunks: Iterator[Chunk],
    service_bus_connection_string: str = None,
    topic_name: str = "ingestion",
) -> int:
    """
    Send Chunk objects to Azure Service Bus topic.
    
    Args:
        chunks: Iterator of Chunk objects to send
        service_bus_connection_string: Service Bus connection string. If None, loads from .env
        topic_name: Name of the Service Bus topic. Default: "ingestion"
    
    Returns:
        Number of chunks sent successfully
    """
    # Load environment variables
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)
    
    if service_bus_connection_string is None:
        service_bus_connection_string = os.getenv("SERVICE_BUS_CONN_STR", "")
        if not service_bus_connection_string:
            raise RuntimeError("SERVICE_BUS_CONN_STR env var is required")
    
    # Connect to Service Bus
    servicebus_client = ServiceBusClient.from_connection_string(service_bus_connection_string)
    sender = servicebus_client.get_topic_sender(topic_name=topic_name)
    
    sent_count = 0
    
    try:
        progress = tqdm(desc="Chunks sent", unit="chunk")
        for chunk in chunks:
            # Serialize chunk to JSON
            message_body = json.dumps(
                {
                    "chunk": chunk.chunk,
                    "id": chunk.id,
                    "doc_id": chunk.doc_id,
                },
                ensure_ascii=False,
            )
            
            # Create and send message
            message = ServiceBusMessage(message_body)
            sender.send_messages(message)
            sent_count += 1
            progress.update(1)
            time.sleep(1)
            
    finally:
        progress.close()
        sender.close()
        servicebus_client.close()
    
    return sent_count
