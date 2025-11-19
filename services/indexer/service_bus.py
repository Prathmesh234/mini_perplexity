import json
import os
from typing import Optional
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from chunk_schema import Chunk
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ServiceBusPublisher:
    """Publisher for sending chunks to Azure Service Bus."""
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize the service bus publisher.
        
        Args:
            connection_string: Azure Service Bus connection string
        """
        load_dotenv()
        
        self.connection_string = connection_string or os.getenv("AZURE_SERVICE_BUS_CONNECTION_STRING")
        if not self.connection_string:
            raise ValueError("Azure Service Bus connection string is required")
            
        self.namespace = "embedding-pipeline-search"
        self.topic_name = "ingestion"
        
        # Initialize Service Bus client
        self.servicebus_client = ServiceBusClient.from_connection_string(self.connection_string)
        
    def send_chunk(self, chunk: Chunk) -> bool:
        """
        Send a single chunk to the service bus topic.
        
        Args:
            chunk: Chunk object to send
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert chunk to JSON
            chunk_json = chunk.model_dump_json()
            
            # Create message
            message = ServiceBusMessage(chunk_json)
            
            # Send message
            with self.servicebus_client.get_topic_sender(topic_name=self.topic_name) as sender:
                sender.send_messages(message)
                
            logger.debug(f"Sent chunk {chunk.chunk_id} to topic {self.topic_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send chunk {chunk.chunk_id}: {e}")
            return False
    
    def test_connection(self) -> bool:
        """
        Test the service bus connection.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            # Try to get a sender to test connection
            with self.servicebus_client.get_topic_sender(topic_name=self.topic_name) as sender:
                logger.info("Service Bus connection test successful")
                return True
        except Exception as e:
            logger.error(f"Service Bus connection test failed: {e}")
            return False
    
    def close(self):
        """Close the service bus client."""
        try:
            self.servicebus_client.close()
            logger.info("Service Bus client closed")
        except Exception as e:
            logger.error(f"Error closing Service Bus client: {e}")
