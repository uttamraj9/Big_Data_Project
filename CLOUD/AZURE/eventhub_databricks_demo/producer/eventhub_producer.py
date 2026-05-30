#!/usr/bin/env python3
"""
Event Hub Producer - Sends sample transaction data to Azure Event Hub
"""

import json
import time
import random
import os
from datetime import datetime, timezone
from pathlib import Path
from azure.eventhub import EventHubProducerClient, EventData

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("⚠️  python-dotenv not installed. Using environment variables only.")
    print("   Install: pip install python-dotenv")

# ============================================
# Configuration
# ============================================
# IMPORTANT: Set these in .env file or as environment variables
# Get connection string from: az eventhubs namespace authorization-rule keys list

EVENTHUB_CONNECTION_STRING = os.getenv(
    "EVENTHUB_CONNECTION_STRING",
    "Endpoint=sb://YOUR-NAMESPACE.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YOUR-KEY-HERE"
)
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME", "demo-transactions")

# Validate configuration
if "YOUR-KEY-HERE" in EVENTHUB_CONNECTION_STRING:
    print("❌ ERROR: Event Hub connection string not configured!")
    print("   Create a .env file with EVENTHUB_CONNECTION_STRING")
    print("   Or set environment variables before running.")
    exit(1)

# Sample data
MERCHANTS = [
    "Amazon UK", "Tesco", "Sainsburys", "Asda", "Morrisons",
    "Apple Store", "Currys", "Argos", "John Lewis", "Marks & Spencer"
]

CATEGORIES = [
    "Shopping", "Groceries", "Electronics", "Clothing", "Entertainment",
    "Dining", "Transportation", "Healthcare", "Utilities", "Other"
]

LOCATIONS = [
    "London, UK", "Manchester, UK", "Birmingham, UK", "Leeds, UK",
    "Liverpool, UK", "Bristol, UK", "Edinburgh, UK", "Glasgow, UK"
]

PAYMENT_METHODS = ["credit_card", "debit_card", "contactless", "mobile_wallet"]


def generate_transaction(transaction_number):
    """Generate a single transaction record"""
    is_fraud = random.random() < 0.05  # 5% fraud rate

    transaction = {
        "transaction_id": f"TXN-{transaction_number:010d}",
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "amount": round(random.uniform(1.0, 999.99) if not is_fraud else random.uniform(1000, 9999), 2),
        "merchant": "Unknown Merchant" if is_fraud else random.choice(MERCHANTS),
        "category": "Other" if is_fraud else random.choice(CATEGORIES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "location": "Unknown" if is_fraud else random.choice(LOCATIONS),
        "is_fraud": is_fraud,
        "payment_method": random.choice(PAYMENT_METHODS)
    }

    return transaction


def send_batch_to_eventhub(producer, transactions):
    """Send a batch of transactions to Event Hub"""
    event_data_batch = producer.create_batch()

    for txn in transactions:
        try:
            # Convert to JSON and create EventData
            json_data = json.dumps(txn)
            event_data_batch.add(EventData(json_data))
        except ValueError:
            # Batch is full, send it and create a new batch
            producer.send_batch(event_data_batch)
            event_data_batch = producer.create_batch()
            event_data_batch.add(EventData(json_data))

    # Send the final batch
    if len(event_data_batch) > 0:
        producer.send_batch(event_data_batch)


def main():
    """Main producer function"""
    print("=" * 60)
    print("Event Hub Producer - Transaction Data")
    print("=" * 60)
    print(f"Event Hub: {EVENTHUB_NAME}")
    print(f"Namespace: ehubnamespacemay2026")
    print("=" * 60)
    print()

    # Create Event Hub producer
    try:
        producer = EventHubProducerClient.from_connection_string(
            conn_str=EVENTHUB_CONNECTION_STRING,
            eventhub_name=EVENTHUB_NAME
        )
        print("✅ Connected to Event Hub")
        print()
    except Exception as e:
        print(f"❌ Failed to connect to Event Hub: {e}")
        return

    try:
        total_sent = 0
        batch_size = 10
        num_batches = 10  # Send 10 batches = 100 transactions

        print(f"Sending {num_batches} batches of {batch_size} transactions each...")
        print()

        for batch_num in range(num_batches):
            # Generate batch of transactions
            transactions = []
            for i in range(batch_size):
                txn_number = total_sent + i + 1
                transactions.append(generate_transaction(txn_number))

            # Send to Event Hub
            send_batch_to_eventhub(producer, transactions)
            total_sent += len(transactions)

            print(f"✅ Batch {batch_num + 1}/{num_batches} sent ({len(transactions)} transactions) - Total: {total_sent}")

            # Small delay between batches
            time.sleep(0.5)

        print()
        print("=" * 60)
        print(f"✅ SUCCESS: Sent {total_sent} transactions to Event Hub")
        print("=" * 60)
        print()
        print("Sample transaction:")
        sample_txn = generate_transaction(9999)
        print(json.dumps(sample_txn, indent=2))
        print()
        print("Next steps:")
        print("  1. Go to Databricks workspace")
        print("  2. Upload and run: databricks/read_eventhub_kafka.py")
        print("  3. Verify data is flowing")

    except Exception as e:
        print(f"❌ Error sending data: {e}")
        import traceback
        traceback.print_exc()

    finally:
        producer.close()
        print()
        print("Producer closed.")


if __name__ == "__main__":
    main()
