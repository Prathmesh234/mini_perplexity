import os
from azure.servicebus.management import ServiceBusAdministrationClient
from dotenv import load_dotenv
from pathlib import Path

# Load env from the same directory
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

conn_str = os.getenv("SERVICE_BUS_CONN_STR")
topic_name = os.getenv("TOPIC_NAME_INGESTION", "ingestion")

if not conn_str:
    print("Error: SERVICE_BUS_CONN_STR not found in .env")
    exit(1)

print(f"Connecting to Service Bus...")
try:
    client = ServiceBusAdministrationClient.from_connection_string(conn_str)
    
    print(f"\n--- Topic: {topic_name} ---")
    try:
        topic_props = client.get_topic_runtime_properties(topic_name)
        print(f"Total Messages in Topic (across all subs): {topic_props.total_message_count}")
    except Exception as e:
        print(f"Could not get topic properties: {e}")

    print(f"\n--- Subscriptions ---")
    subs = client.list_subscriptions(topic_name)
    for sub in subs:
        try:
            sub_props = client.get_subscription_runtime_properties(topic_name, sub.name)
            print(f"Subscription: {sub.name}")
            print(f"  Active Messages:      {sub_props.active_message_count}")
            print(f"  Dead Letter Messages: {sub_props.dead_letter_message_count}")
            print(f"  Transfer Dead Letter: {sub_props.transfer_dead_letter_message_count}")
            print("-" * 20)
        except Exception as e:
            print(f"Error getting stats for subscription {sub.name}: {e}")

except Exception as e:
    print(f"Fatal Error: {e}")
