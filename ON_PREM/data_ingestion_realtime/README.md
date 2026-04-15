# data_ingestion_realtime

Real-time data pipeline for the credit card fraud detection project.
Streams transaction data from a FastAPI endpoint through Kafka into HBase.

```
FastAPI /data  (PostgreSQL source)
      |
      |  HTTP GET  (offset/limit pagination)
      v
kafka_producer.py
      |
      |  JSON messages  (key = Transaction_ID)
      v
Kafka topic: cc_fraud_stream  (3 partitions)
      |
      |  batch consume (50 msg buffer)
      v
kafka_consumer.py  →  HBase table: cc_fraud_realtime
                            row key = Transaction_ID
                            column family = cf
```

---

## Directory Structure

```
data_ingestion_realtime/
├── src/
│   ├── kafka_producer.py    Polls FastAPI /data, publishes JSON to Kafka
│   ├── kafka_consumer.py    Consumes from Kafka, batch-writes to HBase
│   └── hbase_client.py      HBase Thrift wrapper (happybase)
│
├── config/
│   └── config.properties    Broker URL, topic, HBase host, batch sizes
│
├── scripts/
│   ├── start_producer.sh    Wrapper to start the producer (python3.12)
│   └── start_consumer.sh    Wrapper to start the consumer (python3.12)
│
├── tests/
│   ├── conftest.py          pytest fixtures (mocked Kafka, HBase, sample rows)
│   └── test_kafka_pipeline.py  Unit tests — no live cluster required
│
├── Jenkinsfile              Jenkins pipeline: unit tests → deploy → start
├── requirements.txt         Python dependencies
└── README.md                ← this file
```

---

## Configuration

`config/config.properties`:

| Key | Default | Description |
|-----|---------|-------------|
| `api.url` | `http://13.42.152.118:5310` | FastAPI base URL |
| `kafka.brokers` | `172.31.3.85:9092` | Kafka broker list |
| `kafka.topic` | `cc_fraud_stream` | Source/target topic |
| `hbase.host` | `ip-172-31-3-85.eu-west-2.compute.internal` | HBase Thrift host |
| `hbase.port` | `9090` | HBase Thrift port |
| `batch.size` | `100` | Records per API call (producer) |
| `consumer.batch` | `50` | Messages buffered before HBase flush |

All values can be overridden by environment variables or CLI flags — see source file headers for details.

---

## HBase Table Layout

| Property | Value |
|----------|-------|
| Table | `cc_fraud_realtime` |
| Row key | `Transaction_ID` |
| Column family | `cf` |
| Columns | `cf:User_ID`, `cf:Transaction_Amount`, `cf:Transaction_Type`, `cf:Timestamp`, `cf:Account_Balance`, `cf:Device_Type`, `cf:Location`, `cf:Merchant_Category`, `cf:Fraud_Label`, … |

The table is created automatically on first run if it does not exist.

---

## Part 1 — Manual Steps

### Prerequisites

- Python 3.12 on the edge node (`13.41.167.97`)
- Kafka broker reachable at `172.31.3.85:9092`
- HBase Thrift server running on port `9090`
- FastAPI server running at `http://13.42.152.118:5310`

### 1. Install dependencies (edge node)

```bash
ssh -i ~/test_key.pem ec2-user@13.41.167.97

python3.12 -m pip install --user 'setuptools<70'
python3.12 -m pip install --user kafka-python-ng happybase requests python-dotenv
```

### 2. Create Kafka topic (if not already created)

```bash
/opt/cloudera/parcels/CDH/bin/kafka-topics \
  --bootstrap-server 172.31.3.85:9092 \
  --create --topic cc_fraud_stream \
  --partitions 3 --replication-factor 1
```

Verify:

```bash
/opt/cloudera/parcels/CDH/bin/kafka-topics \
  --bootstrap-server 172.31.3.85:9092 --list
```

### 3. Deploy source files

From your local machine:

```bash
EDGE=ec2-user@13.41.167.97
KEY=~/test_key.pem
REMOTE=/tmp/realtime_deploy

ssh -i $KEY $EDGE "mkdir -p $REMOTE/src $REMOTE/config $REMOTE/scripts"

scp -i $KEY data_ingestion_realtime/src/kafka_producer.py \
             data_ingestion_realtime/src/kafka_consumer.py \
             data_ingestion_realtime/src/hbase_client.py \
             $EDGE:$REMOTE/src/

scp -i $KEY data_ingestion_realtime/config/config.properties \
             $EDGE:$REMOTE/config/

scp -i $KEY data_ingestion_realtime/scripts/start_producer.sh \
             data_ingestion_realtime/scripts/start_consumer.sh \
             $EDGE:$REMOTE/scripts/
```

### 4. Start the producer

```bash
ssh -i $KEY $EDGE \
  "chmod +x $REMOTE/scripts/start_producer.sh && \
   nohup bash $REMOTE/scripts/start_producer.sh \
     > $REMOTE/producer.log 2>&1 & \
   echo \$! > $REMOTE/producer.pid && \
   echo 'Producer PID:' \$(cat $REMOTE/producer.pid)"
```

Tail the log:

```bash
ssh -i $KEY $EDGE "tail -f $REMOTE/producer.log"
```

### 5. Start the consumer

```bash
ssh -i $KEY $EDGE \
  "chmod +x $REMOTE/scripts/start_consumer.sh && \
   nohup bash $REMOTE/scripts/start_consumer.sh \
     > $REMOTE/consumer.log 2>&1 & \
   echo \$! > $REMOTE/consumer.pid && \
   echo 'Consumer PID:' \$(cat $REMOTE/consumer.pid)"
```

Tail the log:

```bash
ssh -i $KEY $EDGE "tail -f $REMOTE/consumer.log"
```

### 6. Verify data in HBase

```bash
ssh -i $KEY $EDGE "python3.12 - <<'EOF'
import sys; sys.path.insert(0, '/tmp/realtime_deploy/src')
from hbase_client import HBaseClient
with HBaseClient(host='ip-172-31-3-85.eu-west-2.compute.internal') as c:
    for row in c.scan(limit=5):
        print(row)
EOF"
```

Or via HBase shell:

```bash
hbase shell
> scan 'cc_fraud_realtime', {LIMIT => 5}
```

### 7. Stop the pipeline

```bash
ssh -i $KEY $EDGE \
  "kill \$(cat $REMOTE/producer.pid) \$(cat $REMOTE/consumer.pid)"
```

---

## Part 2 — Jenkins Pipeline

**Pipeline name:** `realtime-layer-ingestion`
**Jenkins URL:** `http://13.42.152.118:8080/job/realtime-layer-ingestion/`
**Jenkinsfile:** `data_ingestion_realtime/Jenkinsfile`

### Pipeline stages

| Stage | Description |
|-------|-------------|
| Install Dependencies | Creates/reuses venv `unit_testing_bd`, installs `requirements.txt` |
| Unit Tests | Runs `pytest tests/` with mocked Kafka/HBase — no cluster needed |
| Start Kafka Producer | SCPs files to edge node, starts producer as background process |
| Start Kafka Consumer → HBase | SCPs files to edge node, starts consumer as background process |

### Running the pipeline

1. Open `http://13.42.152.118:8080/job/realtime-layer-ingestion/`
2. Click **Build Now**
3. The pipeline completes after starting both background processes
4. Monitor logs on the edge node:
   ```bash
   ssh -i ~/test_key.pem ec2-user@13.41.167.97
   tail -f /tmp/realtime_deploy/producer.log
   tail -f /tmp/realtime_deploy/consumer.log
   ```

### Stopping the background processes

```bash
ssh -i ~/test_key.pem ec2-user@13.41.167.97 \
  "kill \$(cat /tmp/realtime_deploy/producer.pid) \$(cat /tmp/realtime_deploy/consumer.pid)"
```

---

## Cluster Details

| Component | Value |
|-----------|-------|
| Edge node | `13.41.167.97` |
| Kafka broker | `172.31.3.85:9092` |
| Kafka topic | `cc_fraud_stream` (3 partitions) |
| HBase Thrift | `ip-172-31-3-85.eu-west-2.compute.internal:9090` |
| HBase table | `cc_fraud_realtime` |
| FastAPI source | `http://13.42.152.118:5310/data` |
| Jenkins | `http://13.42.152.118:8080` |
