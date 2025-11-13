import os
import json
import time
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from azure.servicebus import ServiceBusClient, ServiceBusReceiver, ServiceBusReceivedMessage


def load_env_config() -> Dict[str, str]:
    """Load environment variables from .env file."""
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)
    
    config = {
        'service_bus_conn_str': os.getenv("SERVICE_BUS_CONN_STR", ""),
        'topic_name_ingestion': os.getenv("TOPIC_NAME_INGESTION", "ingestion"),
    }
    
    if not config['service_bus_conn_str']:
        raise RuntimeError("SERVICE_BUS_CONN_STR env var is required")
    
    return config


def read_from_ingestion_topic(
    subscription_name: str = "embedding-subscription",
    max_wait_time: int = 60,
    sleep_between_messages: float = 2.0
) -> None:
    """
    Read messages from the ingestion topic one after another with delays.
    
    Args:
        subscription_name: Name of the subscription to read from
        max_wait_time: Maximum time to wait for a message (seconds)
        sleep_between_messages: Time to sleep between processing messages (seconds)
    """
    config = load_env_config()
    
    # Connect to Service Bus
    servicebus_client = ServiceBusClient.from_connection_string(config['service_bus_conn_str'])
    
    try:
        # Get receiver for the subscription
        receiver = servicebus_client.get_subscription_receiver(
            topic_name=config['topic_name_ingestion'],
            subscription_name=subscription_name,
            max_wait_time=max_wait_time
        )
        
        print(f"Starting to read from topic '{config['topic_name_ingestion']}' subscription '{subscription_name}'...")
        
        with receiver:
            while True:
                try:
                    # Receive messages one at a time
                    received_msgs = receiver.receive_messages(max_message_count=1, max_wait_time=max_wait_time)
                    
                    if not received_msgs:
                        print("No messages received, waiting...")
                        time.sleep(sleep_between_messages)
                        continue
                    
                    for msg in received_msgs:
                        try:
                            # Parse the message body
                            message_data = json.loads(str(msg))
                            
                            # Extract chunk data (based on indexer format)
                            chunk_text = message_data.get('chunk', '')
                            chunk_id = message_data.get('id', '')
                            doc_id = message_data.get('doc_id', '')
                            
                            print(f"Received chunk: ID={chunk_id}, Doc ID={doc_id}")
                            print(f"Chunk preview: {chunk_text[:100]}...")
                            
                            # TODO: Process the chunk data here (convert to embeddings, etc.)
                            
                            # Complete the message (acknowledge receipt)
                            receiver.complete_message(msg)
                            print(f"Message {chunk_id} processed successfully")
                            
                        except json.JSONDecodeError as e:
                            print(f"Failed to parse message JSON: {e}")
                            # Dead letter the message if JSON is invalid
                            receiver.dead_letter_message(msg, reason="Invalid JSON")
                            
                        except Exception as e:
                            print(f"Error processing message: {e}")
                            # Abandon the message so it can be retried
                            receiver.abandon_message(msg)
                    
                    # Sleep between processing messages
                    print(f"Sleeping for {sleep_between_messages} seconds...")
                    time.sleep(sleep_between_messages)
                    
                except KeyboardInterrupt:
                    print("Stopping message processing...")
                    break
                    
                except Exception as e:
                    print(f"Error receiving messages: {e}")
                    time.sleep(sleep_between_messages)
                    
    finally:
        servicebus_client.close()
        print("Service Bus client closed")


if __name__ == "__main__":
    # Run the message reader
    read_from_ingestion_topic()