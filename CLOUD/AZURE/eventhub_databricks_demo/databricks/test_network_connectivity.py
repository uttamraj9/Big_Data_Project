# Databricks notebook source
# MAGIC %md
# MAGIC # Network Connectivity Test
# MAGIC
# MAGIC Tests connectivity from Databricks cluster to Azure Event Hubs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: DNS Resolution

# COMMAND ----------

import socket

EVENTHUB_NAMESPACE = "ehubnamespacemay2026"
EVENTHUB_FQDN = f"{EVENTHUB_NAMESPACE}.servicebus.windows.net"

print(f"Testing DNS resolution for: {EVENTHUB_FQDN}")

try:
    ip_address = socket.gethostbyname(EVENTHUB_FQDN)
    print(f"✅ DNS Resolution SUCCESS")
    print(f"   IP Address: {ip_address}")
except Exception as e:
    print(f"❌ DNS Resolution FAILED: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: TCP Connection to AMQP Port (5671)

# COMMAND ----------

print(f"Testing TCP connection to {EVENTHUB_FQDN}:5671 (AMQP)")

try:
    sock = socket.create_connection((EVENTHUB_FQDN, 5671), timeout=10)
    sock.close()
    print(f"✅ AMQP Port 5671 - REACHABLE")
except Exception as e:
    print(f"❌ AMQP Port 5671 - BLOCKED: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: TCP Connection to Kafka Port (9093)

# COMMAND ----------

print(f"Testing TCP connection to {EVENTHUB_FQDN}:9093 (Kafka)")

try:
    sock = socket.create_connection((EVENTHUB_FQDN, 9093), timeout=10)
    sock.close()
    print(f"✅ Kafka Port 9093 - REACHABLE")
except Exception as e:
    print(f"❌ Kafka Port 9093 - BLOCKED: {e}")
    print(f"\n⚠️  This explains why Kafka connector fails!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: HTTPS (Management API)

# COMMAND ----------

import urllib.request

print(f"Testing HTTPS connection to {EVENTHUB_FQDN}:443")

try:
    response = urllib.request.urlopen(f"https://{EVENTHUB_FQDN}", timeout=10)
    print(f"✅ HTTPS Port 443 - REACHABLE")
    print(f"   Status: {response.status}")
except Exception as e:
    print(f"❌ HTTPS Port 443 - Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: Cluster Network Configuration

# COMMAND ----------

# Check outbound internet access
print("Testing outbound internet connectivity...")

try:
    socket.create_connection(("www.google.com", 443), timeout=5)
    print("✅ Outbound internet access OK")
except:
    print("❌ No outbound internet access")

# Check cluster details
print("\n--- Cluster Configuration ---")
print(f"Spark Version: {spark.version}")

# Try to get network info
import subprocess
try:
    result = subprocess.run(['hostname', '-I'], capture_output=True, text=True)
    print(f"Cluster IP: {result.stdout.strip()}")
except:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 70)
print("NETWORK CONNECTIVITY TEST RESULTS")
print("=" * 70)
print("\n📋 Tests Performed:")
print("  1. DNS Resolution for Event Hubs")
print("  2. TCP Connection to AMQP Port 5671")
print("  3. TCP Connection to Kafka Port 9093")
print("  4. HTTPS Connection Port 443")
print("  5. General Internet Connectivity")
print("\n" + "=" * 70)
print("\n💡 Recommendations:")
print("  - If Kafka port 9093 is blocked:")
print("    → Use Event Hubs Capture to ADLS (workaround)")
print("    → Configure VNet Service Endpoints")
print("    → Enable Private Link for Event Hubs")
print("  - If AMQP port 5671 works:")
print("    → Use azure-eventhub Python SDK instead of Kafka")
print("=" * 70)
