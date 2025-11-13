"""Command-line entry point for the indexer pipeline."""

import argparse

from chunk_index import chunk_index
from embedding_ingestion import send_chunks_to_service_bus


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Chunk CommonCrawl data and send to the embedding ingestion topic"
    )

    parser.add_argument(
        "--azure-connection-string",
        dest="azure_conn_str",
        help="Override AZURE_CONN_STR env var for blob access",
    )
    parser.add_argument(
        "--container-name",
        dest="container_name",
        help="Override CONTAINER_NAME env var",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1500,
        help="Approximate character size per chunk (default: 1500)",
    )
    parser.add_argument(
        "--overlap",
        type=int,
        default=200,
        help="Character overlap between successive chunks (default: 200)",
    )
    parser.add_argument(
        "--service-bus-connection-string",
        dest="service_bus_conn_str",
        help="Override SERVICE_BUS_CONN_STR env var",
    )
    parser.add_argument(
        "--topic-name",
        default="ingestion",
        help="Service Bus topic name (default: ingestion)",
    )

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    chunk_kwargs = {
        "chunk_size": args.chunk_size,
        "overlap": args.overlap,
    }
    if args.azure_conn_str:
        chunk_kwargs["azure_connection_string"] = args.azure_conn_str
    if args.container_name:
        chunk_kwargs["container_name"] = args.container_name

    send_kwargs = {
        "topic_name": args.topic_name,
    }
    if args.service_bus_conn_str:
        send_kwargs["service_bus_connection_string"] = args.service_bus_conn_str

    sent = send_chunks_to_service_bus(
        chunks=chunk_index(**chunk_kwargs),
        **send_kwargs,
    )

    print(
        f"Sent {sent} chunks to Service Bus topic '{send_kwargs['topic_name']}'"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
