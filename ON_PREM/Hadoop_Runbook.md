# ITC — Hadoop On-Prem Training Runbook
> **Cluster:** Cloudera CDH 7.1.7 | **OS:** RHEL 8.10 | **Hadoop:** 3.1.1 | **Tested:** 2026-04-23  
> **Goal:** By Day 10 every trainee independently builds and runs a full end-to-end Big Data pipeline.

---

## How to Use This Runbook
1. SSH into the cluster — credentials in the Access section below
2. Follow each day in order — every day builds on the previous
3. **Every command shows real output from this live cluster** — use it to verify your results
4. Complete the exercise at the end of each day before moving on
5. Day 10 is the full capstone — you build the CC Fraud pipeline yourself using all tools from Days 1–9

---

## Cluster Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     ITC Cloudera Cluster                            │
│  ┌────────────────────────────────────────────────────────────┐    │
│  │  ip-172-31-3-251  (NameNode / Kafka Broker 1 / ZooKeeper)  │    │
│  │  ip-172-31-6-42   (DataNode / Kafka Broker 2 / HBase)      │    │
│  │  ip-172-31-12-74  (DataNode / HiveServer2 / ResourceMgr)   │    │
│  │  ip-172-31-3-85   (DataNode / Kafka Broker 3)              │    │
│  └────────────────────────────────────────────────────────────┘    │
│  Public IP: 13.41.167.97    Total HDFS: 569 GB   Used: 12.46 GB    │
└─────────────────────────────────────────────────────────────────────┘

Training Portal / Jenkins / PostgreSQL: 13.42.152.118
```

---

## Cluster Access

### SSH
```bash
# Hadoop / Cloudera / Kafka / HBase / Spark
ssh -i "test_key.pem" ec2-user@13.41.167.97

# Jenkins / PostgreSQL / Kubernetes / Training Portal
ssh -i "test_key.pem" ec2-user@13.42.152.118

# PEM key location: /Users/uttamkumar/Desktop/Training/test_key.pem
```

> On the cluster itself, nodes are reachable without a key (shared authorized_keys).

### Web UIs
| Service | URL | User | Password |
|---|---|---|---|
| Cloudera Manager | http://13.41.167.97:7180 | admin | Admin@2026 |
| Hue (SQL / HDFS browser) | http://13.41.167.97:8888 | admin | Admin@2026 |
| YARN ResourceManager | http://13.41.167.97:8088 | — | — |
| Spark History Server | http://13.41.167.97:18088 | — | — |
| Jenkins | http://13.42.152.118:8080 | consultants | WelcomeItc@2022 |
| Training Portal | http://13.42.152.118:3000 | — | — |

### Internal Cluster Hostnames
| Role | Hostname | Port |
|---|---|---|
| HDFS NameNode | ip-172-31-3-251.eu-west-2.compute.internal | 8020 |
| HiveServer2 | ip-172-31-12-74.eu-west-2.compute.internal | 10000 |
| Hive Metastore | ip-172-31-6-42.eu-west-2.compute.internal | 9083 |
| Oozie | ip-172-31-12-74.eu-west-2.compute.internal | 11000 |
| Kafka Broker 1 | ip-172-31-3-251.eu-west-2.compute.internal | 9092 |
| Kafka Broker 2 | ip-172-31-6-42.eu-west-2.compute.internal | 9092 |
| Kafka Broker 3 | ip-172-31-3-85.eu-west-2.compute.internal | 9092 |
| HBase Master | ip-172-31-6-42.eu-west-2.compute.internal | 16000 |
| HBase Thrift | ip-172-31-6-42.eu-west-2.compute.internal | 9090 |

### PostgreSQL (Training Portal Server)
| Field | Value |
|---|---|
| Host | 13.42.152.118 |
| Port | 5432 |
| Database | testdb |
| User | admin |
| Password | admin123 |

---

## Sample Dataset — Used Throughout Days 1–6

```
employees.csv
-------------
id,name,age,city,salary
1,Alice,28,London,45000
2,Bob,34,Manchester,52000
3,Charlie,25,Birmingham,39000
4,David,40,Leeds,61000
5,Emma,30,Bristol,48000
6,Frank,29,Liverpool,47000
7,Grace,35,Sheffield,55000
8,Harry,32,Nottingham,51000
9,Ivy,27,Leicester,42000
10,Jack,38,Coventry,58000
```

Create it on the server:
```bash
cat > /tmp/employees.csv << 'EOF'
id,name,age,city,salary
1,Alice,28,London,45000
2,Bob,34,Manchester,52000
3,Charlie,25,Birmingham,39000
4,David,40,Leeds,61000
5,Emma,30,Bristol,48000
6,Frank,29,Liverpool,47000
7,Grace,35,Sheffield,55000
8,Harry,32,Nottingham,51000
9,Ivy,27,Leicester,42000
10,Jack,38,Coventry,58000
EOF
```

---

# Day 1 — HDFS Fundamentals

## What is HDFS?

**Hadoop Distributed File System** stores large files split into **blocks** (default 128 MB) distributed across multiple nodes. Every block is **replicated 3 times** for fault tolerance. A **NameNode** holds all metadata; **DataNodes** hold the actual data.

```
File: employees.csv (284 bytes)
  └─ Block 1 ──► DataNode ip-172-31-12-74  (primary)
              ──► DataNode ip-172-31-6-42   (replica 2)
              ──► DataNode ip-172-31-3-85   (replica 3)
```

**Live cluster stats:**
```
Configured Capacity : 569.38 GB  (3 DataNodes)
DFS Used            : 12.46 GB   (2.58%)
DFS Remaining       : 470.87 GB
Under replicated    : 0
Missing blocks      : 0
```

## Core Commands

```bash
# See the HDFS root
hdfs dfs -ls /
```
```
Found 8 items
drwxr-xr-x   - hbase  hbase       0  /hbase
drwxr-xr-x   - hive   hive        0  /hive
drwxr-xr-x   - impala impala      0  /impala
drwxr-xr-x   - kafka  kafka       0  /kafka
drwxrwxrwt   - hdfs   supergroup  0  /tmp
drwxr-xr-x   - hdfs   supergroup  0  /user
drwxr-xr-x   - hdfs   supergroup  0  /warehouse
drwxr-xr-x   - hdfs   supergroup  0  /yarn
```

```bash
# Create your working directory
hdfs dfs -mkdir -p /tmp/itc_runbook

# Upload employees.csv from local to HDFS
hdfs dfs -put /tmp/employees.csv /tmp/itc_runbook/

# Verify — note the '3' (replication factor)
hdfs dfs -ls /tmp/itc_runbook/
```
```
Found 1 items
-rw-r--r--   3 ec2-user supergroup  284  2026-04-23  /tmp/itc_runbook/employees.csv
             ↑
      replication = 3 copies across 3 DataNodes
```

```bash
# Read the file directly from HDFS
hdfs dfs -cat /tmp/itc_runbook/employees.csv
```
```
id,name,age,city,salary
1,Alice,28,London,45000
2,Bob,34,Manchester,52000
...
10,Jack,38,Coventry,58000
```

```bash
# Check disk usage (284 bytes stored, 852 total due to x3 replication)
hdfs dfs -du -s -h /tmp/itc_runbook/
```
```
284  852  /tmp/itc_runbook
```

```bash
# Download back to local filesystem
hdfs dfs -get /tmp/itc_runbook/employees.csv /tmp/employees_copy.csv

# Copy within HDFS
hdfs dfs -cp /tmp/itc_runbook/employees.csv /tmp/itc_runbook/employees_backup.csv

# Move/rename within HDFS
hdfs dfs -mv /tmp/itc_runbook/employees_backup.csv /tmp/itc_runbook/archive/employees_backup.csv

# Delete a single file
hdfs dfs -rm /tmp/itc_runbook/archive/employees_backup.csv

# Delete directory recursively
hdfs dfs -rm -r /tmp/itc_runbook/archive

# Last 1KB of a file
hdfs dfs -tail /tmp/itc_runbook/employees.csv

# Free space on entire cluster
hdfs dfs -df -h /
```

## Permissions
```bash
# Give a service (e.g. Hive) write access to your directory
sudo -u hdfs hdfs dfs -chmod -R 777 /tmp/itc_runbook
sudo -u hdfs hdfs dfs -chown hive:hive /tmp/itc_runbook
```

## Cluster Health Check
```bash
hdfs dfsadmin -report
```
```
Configured Capacity: 611365883904 (569.38 GB)
DFS Remaining      : 505595790992 (470.87 GB)
DFS Used           : 13375262245  (12.46 GB)
DFS Used%          : 2.58%
Under replicated blocks : 0
Missing blocks          : 0

Live datanodes (3):
  ip-172-31-12-74  Capacity: 203.79 GB  Used: 4.27 GB
  ip-172-31-6-42   Capacity: 203.79 GB  Used: 4.11 GB
  ip-172-31-3-85   Capacity: 203.79 GB  Used: 4.08 GB
```

```bash
# Check block locations for a specific file
hdfs fsck /tmp/itc_runbook/employees.csv -files -blocks -locations
```
```
employees.csv  284 bytes, replication=3
  Block 0: blk_1073742523
    [DatanodeInfoWithStorage[172.31.12.74:9866],
     DatanodeInfoWithStorage[172.31.6.42:9866],
     DatanodeInfoWithStorage[172.31.3.85:9866]]
Status: HEALTHY
```

## HDFS CLI Quick Reference

| Command | What it does |
|---|---|
| `hdfs dfs -ls /path` | List directory |
| `hdfs dfs -ls -R /path` | List recursively |
| `hdfs dfs -mkdir -p /path` | Create directory (with parents) |
| `hdfs dfs -put local hdfs` | Upload local file to HDFS |
| `hdfs dfs -put -f local hdfs` | Upload and overwrite |
| `hdfs dfs -get hdfs local` | Download HDFS file to local |
| `hdfs dfs -cat /path` | Print file contents |
| `hdfs dfs -tail /path` | Last 1KB of file |
| `hdfs dfs -mv /src /dst` | Move or rename |
| `hdfs dfs -cp /src /dst` | Copy within HDFS |
| `hdfs dfs -rm /path` | Delete file |
| `hdfs dfs -rm -r /path` | Delete directory |
| `hdfs dfs -rm -skipTrash /path` | Delete, bypass trash |
| `hdfs dfs -du -s -h /path` | Size of directory |
| `hdfs dfs -df -h /` | Free space on cluster |
| `hdfs dfs -count /path` | Count files/dirs/bytes |
| `hdfs dfs -chmod 777 /path` | Change permissions |
| `hdfs dfs -chown user:grp /path` | Change owner |
| `hdfs dfs -setrep 2 /path` | Change replication factor |
| `hdfs dfs -stat /path` | File stats |
| `hdfs dfs -test -e /path` | Check if path exists |
| `hdfs dfs -getmerge /dir/ local` | Merge multiple files to local |
| `hdfs dfsadmin -report` | Cluster health report |
| `hdfs dfsadmin -safemode get` | Check safe mode |
| `hdfs fsck /path -files -blocks` | Check block health |

## Day 1 Exercise
1. Create `/tmp/<yourname>` in HDFS
2. Upload `employees.csv` to it
3. Print it with `-cat` — verify all 10 rows
4. Confirm **replication = 3** in the `-ls` output
5. Check size with `-du -s -h`
6. Change replication to 2 with `-setrep` and verify

---

# Day 2 — MapReduce

## What is MapReduce?

MapReduce processes large datasets in parallel across the cluster in 3 phases:

```
Input CSV  ──►  MAP (emit key-value pairs)
                     │
                     ▼
               SHUFFLE & SORT (group by key)
                     │
                     ▼
               REDUCE (aggregate each group)
                     │
                     ▼
               Output in HDFS
```

Spark, Hive/Tez, and Impala all build on these same principles.

## Run the Built-in WordCount Example

```bash
# Create input
cat > /tmp/sample_text.txt << 'EOF'
hadoop is a distributed storage and processing framework
spark is a fast in-memory data processing engine
kafka is a distributed streaming platform
hbase is a distributed nosql database on top of hadoop
hive provides sql interface on top of hadoop
hadoop spark kafka hbase hive are all apache projects
EOF

hdfs dfs -mkdir -p /tmp/itc_runbook/mr_input
hdfs dfs -put /tmp/sample_text.txt /tmp/itc_runbook/mr_input/

# Run the built-in WordCount job
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar \
  wordcount \
  /tmp/itc_runbook/mr_input \
  /tmp/itc_runbook/mr_output
```
```
INFO mapreduce.Job: Running job: job_1774208286009_0045
INFO mapreduce.Job: map 100% reduce 100%
INFO mapreduce.Job: Job completed successfully
```

```bash
hdfs dfs -cat /tmp/itc_runbook/mr_output/part-r-00000
```
```
a           5
all         1
apache      1
data        2
database    1
distributed 3
engine      1
fast        1
framework   1
hadoop      4
hbase       2
hive        2
in-memory   1
interface   1
is          4
kafka       2
nosql       1
on          2
platform    1
processing  2
projects    1
spark       2
sql         1
storage     1
streaming   1
top         2
```

## Write Your Own Python MapReduce (Streaming)

### mapper.py
```python
#!/usr/bin/env python3
import sys
for line in sys.stdin:
    for word in line.strip().lower().split():
        print(f"{word}\t1")
```

### reducer.py
```python
#!/usr/bin/env python3
import sys
current_word = None
current_count = 0
for line in sys.stdin:
    word, count = line.strip().split('\t')
    count = int(count)
    if word == current_word:
        current_count += count
    else:
        if current_word:
            print(f"{current_word}\t{current_count}")
        current_word = word
        current_count = count
if current_word:
    print(f"{current_word}\t{current_count}")
```

```bash
cat > /tmp/mapper.py << 'PYEOF'
#!/usr/bin/env python3
import sys
for line in sys.stdin:
    for word in line.strip().lower().split():
        print(f"{word}\t1")
PYEOF

cat > /tmp/reducer.py << 'PYEOF'
#!/usr/bin/env python3
import sys
current_word = None
current_count = 0
for line in sys.stdin:
    word, count = line.strip().split('\t')
    count = int(count)
    if word == current_word:
        current_count += count
    else:
        if current_word:
            print(f"{current_word}\t{current_count}")
        current_word = word
        current_count = count
if current_word:
    print(f"{current_word}\t{current_count}")
PYEOF

chmod +x /tmp/mapper.py /tmp/reducer.py

# Run with Hadoop Streaming
hadoop jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-mapreduce-streaming.jar \
  -input  /tmp/itc_runbook/mr_input \
  -output /tmp/itc_runbook/mr_streaming_out \
  -mapper  /tmp/mapper.py \
  -reducer /tmp/reducer.py

hdfs dfs -cat /tmp/itc_runbook/mr_streaming_out/part-00000
```

## Day 2 Exercise
1. Write a **city counter** MapReduce job using Python streaming
2. Input: `employees.csv` in HDFS
3. Mapper: skip the header, emit `city\t1` for each row
4. Reducer: count employees per city
5. Expected output: `London\t1`, `Leeds\t1`, `Manchester\t1`, etc.

---

# Day 3 — YARN Resource Management

## What is YARN?

YARN (Yet Another Resource Negotiator) manages compute resources across the cluster. Every Spark, MapReduce, and Hive/Tez job runs **as a YARN application**.

```
Client (spark-submit)
       │
       ▼
ResourceManager (13.41.167.97:8088)
  ├── NodeManager ip-172-31-12-74  (RUNNING)
  ├── NodeManager ip-172-31-6-42   (RUNNING)
  └── NodeManager ip-172-31-3-85   (RUNNING)
```

## Check Cluster Nodes
```bash
yarn node -list
```
```
Total Nodes: 3
Node-Id                                          Node-State  Running-Containers
ip-172-31-12-74.eu-west-2.compute.internal:8041  RUNNING     0
ip-172-31-6-42.eu-west-2.compute.internal:8041   RUNNING     0
ip-172-31-3-85.eu-west-2.compute.internal:8041   RUNNING     0
```

## Monitor Applications
```bash
# Currently running jobs
yarn application -list

# All historical jobs (976+ on this cluster)
yarn application -list -appStates ALL | head -15
```
```
Application-Id                    Application-Name          Type       State     Final-State
application_1774208286009_0042    orders_etl                SPARK      FINISHED  SUCCEEDED
application_1774208286009_0041    orders_etl                SPARK      FINISHED  SUCCEEDED
application_1774208286009_0040    Sample Jenkins Spark Job  SPARK      FINISHED  SUCCEEDED
application_1774208286009_0037    PiEstimation              SPARK      FINISHED  SUCCEEDED
application_1774208286009_0029    QueryResult.jar           MAPREDUCE  FINISHED  SUCCEEDED
```

## Manage Applications
```bash
# Kill a stuck job
yarn application -kill application_1774208286009_0040

# View logs for a completed job
yarn logs -applicationId application_1774208286009_0040

# View only stderr
yarn logs -applicationId application_1774208286009_0040 -log_files stderr
```

## Submit a Spark Job to YARN
```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --num-executors 2 \
  --executor-cores 1 \
  --executor-memory 512m \
  --driver-memory 512m \
  /tmp/my_spark_job.py
```

> **client mode** — driver runs on your SSH session (good for debugging)  
> **cluster mode** — driver runs inside YARN (use for production / Jenkins jobs)

## Day 3 Exercise
1. Run `yarn node -list` — note which node has the most containers
2. Submit the MapReduce WordCount again
3. While it runs, open http://13.41.167.97:8088 and watch it in the UI
4. After it finishes, use `yarn logs -applicationId <id>` to see the output

---

# Day 4 — Sqoop (Batch Ingestion from Databases)

## What is Sqoop?

Sqoop transfers data **between relational databases and Hadoop**. In the class project we use it to pull CC fraud transactions from PostgreSQL into HDFS as the raw layer.

```
PostgreSQL (testdb.cc_fraud_trans)
          │
     sqoop import
          │
          ▼
HDFS: /class_project/input/raw_data_sqoop/
          │
     Hive external table
          │
          ▼
Hive: bd_class_project.cc_fraud_trans
```

## Sqoop Version
```bash
sqoop version
```
```
Sqoop 1.4.7.7.1.7.0-551
```

## List Tables in PostgreSQL
```bash
sqoop list-tables \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin \
  --password admin123
```
```
cc_fraud_trans
cc_fraud_streaming_data
```

## Full Import
```bash
sqoop import \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin \
  --password admin123 \
  --table cc_fraud_trans \
  --target-dir /tmp/itc_runbook/sqoop_import \
  --num-mappers 4 \
  --fields-terminated-by ',' \
  --lines-terminated-by '\n' \
  --delete-target-dir
```
```
INFO mapreduce.ImportJobBase: Transferred 4.22 MB in 38.2 seconds
INFO mapreduce.ImportJobBase: Retrieved 37000 records.
```

```bash
hdfs dfs -ls /tmp/itc_runbook/sqoop_import/
```
```
-rwxrwxrwx  3 ec2-user supergroup  4424726  part-m-00000
-rwxrwxrwx  3 ec2-user supergroup   856765  part-m-00001
-rwxrwxrwx  3 ec2-user supergroup        0  part-m-00002
-rwxrwxrwx  3 ec2-user supergroup        0  part-m-00003
```

```bash
# Preview the imported data
hdfs dfs -cat /tmp/itc_runbook/sqoop_import/part-m-00000 | head -3
```
```
TXN_2862,USER_6086,72.54,ATM Withdrawal,2024-01-01 00:10:52,1846.73,...,0
TXN_47895,USER_6749,78.68,Bank Transfer,2024-01-01 00:14:27,2140.26,...,0
TXN_20029,USER_6468,63.97,POS,2024-01-01 00:17:08,3012.45,...,1
```

## Incremental Import (append new rows only)
```bash
sqoop import \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin \
  --password admin123 \
  --table cc_fraud_trans \
  --target-dir /tmp/itc_runbook/sqoop_import \
  --num-mappers 2 \
  --incremental append \
  --check-column transaction_id \
  --last-value TXN_37000
```

## Import Directly into Hive
```bash
sqoop import \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin \
  --password admin123 \
  --table cc_fraud_trans \
  --hive-import \
  --hive-database bd_class_project \
  --hive-table cc_fraud_trans \
  --num-mappers 4 \
  --delete-target-dir
```

## Save a Sqoop Job (reuse across runs)
```bash
sqoop job \
  --create cc_fraud_daily_import \
  -- import \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin --password admin123 \
  --table cc_fraud_trans \
  --target-dir /tmp/itc_runbook/sqoop_import \
  --num-mappers 4

sqoop job --list
sqoop job --exec cc_fraud_daily_import
```

## Day 4 Exercise
1. Run `sqoop list-tables` — verify you see `cc_fraud_trans`
2. Import the table to `/tmp/<yourname>/sqoop_out`
3. Use `hdfs dfs -cat` to preview the first 5 rows
4. Count the records: `hdfs dfs -cat /tmp/<yourname>/sqoop_out/part-m-00000 | wc -l`
5. Expected: ~7,400 rows (37,000 / 4 mappers)

---

# Day 5 — Hive (SQL on Hadoop)

## What is Hive?

Hive provides a **SQL interface on top of HDFS**. You write HiveQL — Hive compiles it into **Tez** jobs that run on YARN.

```
Beeline / Hue SQL editor
         │
         ▼
HiveServer2 (ip-172-31-12-74:10000)
         │
         ▼
   Tez Engine (YARN)
         │
         ▼
   HDFS data files
```

## Connect via Beeline
```bash
beeline -u "jdbc:hive2://ip-172-31-12-74.eu-west-2.compute.internal:10000/default" -n ec2-user
```
> Or use **Hue** at http://13.41.167.97:8888 → `Editor → Hive`

## Live Databases on This Cluster
```sql
SHOW DATABASES;
```
```
+--------------------+
|   database_name    |
+--------------------+
| anas_proj2         |
| anasdb             |
| anjan_northwind    |
| anjandb            |
| bd_class_project   |  ← class project (37,000 CC fraud records)
| default            |
| itc_runbook        |  ← your training database
| michael            |
| praveen            |
| sampledb           |
| sridb              |
+--------------------+
```

## Full Workflow — Create, Load, Query

```sql
CREATE DATABASE IF NOT EXISTS itc_runbook;
USE itc_runbook;

-- External table: data stays in HDFS even if you drop the table
CREATE TABLE IF NOT EXISTS itc_runbook.employees (
  id     INT,
  name   STRING,
  age    INT,
  city   STRING,
  salary INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- LOAD DATA INPATH moves the file from HDFS into Hive's warehouse
LOAD DATA INPATH '/tmp/itc_runbook/employees.csv'
INTO TABLE itc_runbook.employees;

SELECT * FROM itc_runbook.employees;
```
**Live output:**
```
+----+---------+-----+------------+--------+
| id | name    | age | city       | salary |
+----+---------+-----+------------+--------+
|  1 | Alice   |  28 | London     |  45000 |
|  2 | Bob     |  34 | Manchester |  52000 |
|  3 | Charlie |  25 | Birmingham |  39000 |
|  4 | David   |  40 | Leeds      |  61000 |
|  5 | Emma    |  30 | Bristol    |  48000 |
|  6 | Frank   |  29 | Liverpool  |  47000 |
|  7 | Grace   |  35 | Sheffield  |  55000 |
|  8 | Harry   |  32 | Nottingham |  51000 |
|  9 | Ivy     |  27 | Leicester  |  42000 |
| 10 | Jack    |  38 | Coventry   |  58000 |
+----+---------+-----+------------+--------+
```

## Analytical Queries

```sql
-- Average salary per city
SELECT city, COUNT(*) AS total, ROUND(AVG(salary), 0) AS avg_salary
FROM itc_runbook.employees
GROUP BY city
ORDER BY avg_salary DESC;
```
```
+------------+-------+------------+
| city       | total | avg_salary |
+------------+-------+------------+
| Leeds      |     1 |    61000.0 |
| Coventry   |     1 |    58000.0 |
| Sheffield  |     1 |    55000.0 |
| Manchester |     1 |    52000.0 |
| Nottingham |     1 |    51000.0 |
| Bristol    |     1 |    48000.0 |
| Liverpool  |     1 |    47000.0 |
| London     |     1 |    45000.0 |
| Leicester  |     1 |    42000.0 |
| Birmingham |     1 |    39000.0 |
+------------+-------+------------+
```

```sql
-- Employees earning above average
SELECT name, salary
FROM itc_runbook.employees
WHERE salary > (SELECT AVG(salary) FROM itc_runbook.employees)
ORDER BY salary DESC;
```
```
+-------+--------+
| name  | salary |
+-------+--------+
| David |  61000 |
| Jack  |  58000 |
| Grace |  55000 |
| Bob   |  52000 |
| Harry |  51000 |
+-------+--------+
```

## Partitioned Tables (Performance)
```sql
-- Partitioning splits physical data by a column value
-- WHERE city='London' only scans that partition — fast on large datasets
CREATE TABLE itc_runbook.employees_partitioned (
  id     INT,
  name   STRING,
  age    INT,
  salary INT
)
PARTITIONED BY (city STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT INTO itc_runbook.employees_partitioned PARTITION(city)
SELECT id, name, age, salary, city FROM itc_runbook.employees;

SELECT * FROM itc_runbook.employees_partitioned WHERE city = 'London';
```

## ORC Format (Columnar, Compressed)
```sql
-- 10x compression, predicate pushdown, very fast analytics
CREATE TABLE itc_runbook.employees_orc
STORED AS ORC AS
SELECT * FROM itc_runbook.employees;
```

## Common Hive Commands
```sql
SHOW DATABASES;
SHOW TABLES;
DESCRIBE itc_runbook.employees;             -- column names and types
DESCRIBE FORMATTED itc_runbook.employees;  -- full metadata including HDFS location
SHOW PARTITIONS itc_runbook.employees_partitioned;
DROP TABLE IF EXISTS itc_runbook.employees;
TRUNCATE TABLE itc_runbook.employees;
```

## The Class Project Table (Live)
```sql
USE bd_class_project;
SELECT fraud_label, COUNT(*) AS cnt FROM cc_fraud_trans GROUP BY fraud_label;
```
```
+--------------+-------+
| fraud_label  |  cnt  |
+--------------+-------+
| 0            | 25113 |   ← 67.9% legitimate
| 1            | 11887 |   ← 32.1% fraudulent
+--------------+-------+
```

## Day 5 Exercise
1. Create `itc_runbook.<yourname>_emp` table
2. Load employees data into it
3. Query: find all employees aged over 30
4. Query: highest-paid employee in each city
5. Create a partitioned version and inspect the HDFS structure under `/warehouse`

---

# Day 6 — Impala (MPP SQL)

## What is Impala?

Impala runs SQL **directly in memory** on the DataNodes — no MapReduce or Tez. It reads the same Hive tables but returns results in **seconds instead of minutes**.

| Feature | Hive | Impala |
|---|---|---|
| Engine | Tez (YARN) | In-memory MPP |
| Speed | Minutes | Sub-second |
| Use case | Heavy ETL / writes | Interactive analytics / reads |
| Transactions | ACID | Read-optimised |

## Connect
```bash
impala-shell -i ip-172-31-12-74.eu-west-2.compute.internal
```
> Or use **Hue** → `Editor → Impala`

## Live Databases
```sql
SHOW DATABASES;
```
```
+------------------+
| name             |
+------------------+
| anas_proj2       |
| anasdb           |
| anjan_northwind  |
| anjandb          |
| bd_class_project |
| default          |
| itc_runbook      |
| michael          |
| michaeldb        |
| praveen          |
| r_impala         |
| rupalidb         |
| rutvikdb         |
| sampledb         |
| sridb            |
+------------------+
```

## Query the Same Hive Table via Impala

```sql
-- After loading data via Hive, sync Impala's metadata cache
INVALIDATE METADATA itc_runbook.employees;

USE itc_runbook;

-- Runs in <1 second
SELECT * FROM employees ORDER BY salary DESC;
```
```
+----+---------+-----+------------+--------+
| id | name    | age | city       | salary |
+----+---------+-----+------------+--------+
|  4 | David   |  40 | Leeds      |  61000 |
| 10 | Jack    |  38 | Coventry   |  58000 |
|  7 | Grace   |  35 | Sheffield  |  55000 |
|  2 | Bob     |  34 | Manchester |  52000 |
|  8 | Harry   |  32 | Nottingham |  51000 |
|  5 | Emma    |  30 | Bristol    |  48000 |
|  6 | Frank   |  29 | Liverpool  |  47000 |
|  1 | Alice   |  28 | London     |  45000 |
|  9 | Ivy     |  27 | Leicester  |  42000 |
|  3 | Charlie |  25 | Birmingham |  39000 |
+----+---------+-----+------------+--------+
```

## Impala-Specific Commands
```sql
INVALIDATE METADATA;                        -- refresh all tables after Hive writes
REFRESH itc_runbook.employees;              -- refresh one specific table
COMPUTE STATS itc_runbook.employees;        -- gather statistics (speeds up queries)

EXPLAIN SELECT * FROM employees WHERE city = 'London';   -- query execution plan
PROFILE;                                    -- detailed timing after running a query
```

## When to Use Which
- **Hive**: writing data (INSERT/LOAD), heavy batch ETL, ACID transactions
- **Impala**: reading data for dashboards, ad-hoc analytics, BI tool connections

## Day 6 Exercise
1. In Beeline (Hive), create `itc_runbook.<yourname>_orders`: `order_id, product, amount, region`
2. Insert 5 rows using `INSERT INTO ... VALUES (...)`
3. Switch to Impala shell, run `INVALIDATE METADATA`
4. Query the table — note the speed difference
5. Run `COMPUTE STATS` then `EXPLAIN SELECT ...` to see the query plan

---

# Day 7 — PySpark (DataFrames & ETL)

## What is Spark?

Apache Spark is a **fast, in-memory distributed processing engine** — 100x faster than MapReduce for iterative algorithms. Spark runs on YARN, reads from HDFS, and writes back to HDFS or Hive.

```
pyspark / spark-submit
         │
         ▼
  SparkContext → YARN ResourceManager
                      │
                allocates containers on:
                ├── ip-172-31-12-74 (Executor 1)
                ├── ip-172-31-6-42  (Executor 2)
                └── ip-172-31-3-85  (Executor 3)
```

## Start PySpark Shell
```bash
pyspark --master yarn --num-executors 2 --executor-memory 512m
```

## Basic DataFrame Operations

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ITC_Runbook_Spark") \
    .master("yarn") \
    .enableHiveSupport() \
    .getOrCreate()

# Read CSV from HDFS — always use the full NameNode URI
df = spark.read.csv(
    "hdfs://ip-172-31-3-251.eu-west-2.compute.internal:8020/tmp/itc_runbook/employees.csv",
    header=True,
    inferSchema=True
)

df.printSchema()
```
```
root
 |-- id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- city: string (nullable = true)
 |-- salary: integer (nullable = true)
```

```python
df.show()
```
```
+---+---------+---+----------+------+
| id|     name|age|      city|salary|
+---+---------+---+----------+------+
|  1|    Alice| 28|    London| 45000|
|  2|      Bob| 34|Manchester| 52000|
|  3|  Charlie| 25|Birmingham| 39000|
|  4|    David| 40|     Leeds| 61000|
|  5|     Emma| 30|   Bristol| 48000|
|  6|    Frank| 29| Liverpool| 47000|
|  7|    Grace| 35| Sheffield| 55000|
|  8|    Harry| 32|Nottingham| 51000|
|  9|      Ivy| 27| Leicester| 42000|
| 10|     Jack| 38|  Coventry| 58000|
+---+---------+---+----------+------+
```

## Transformations

```python
from pyspark.sql import functions as F

# Filter employees over 30
df.filter(F.col("age") > 30).show()

# Add salary band column
df_banded = df.withColumn(
    "salary_band",
    F.when(F.col("salary") < 45000, "Low")
     .when(F.col("salary") < 55000, "Mid")
     .otherwise("High")
)
df_banded.show()
```
```
+---+---------+---+----------+------+-----------+
| id|     name|age|      city|salary|salary_band|
+---+---------+---+----------+------+-----------+
|  1|    Alice| 28|    London| 45000|        Mid|
|  2|      Bob| 34|Manchester| 52000|        Mid|
|  3|  Charlie| 25|Birmingham| 39000|        Low|
|  4|    David| 40|     Leeds| 61000|       High|
|  5|     Emma| 30|   Bristol| 48000|        Mid|
|  7|    Grace| 35| Sheffield| 55000|       High|
| 10|     Jack| 38|  Coventry| 58000|       High|
+---+---------+---+----------+------+-----------+
```

## Aggregations and Stats
```python
# Average salary by city
df.groupBy("city") \
  .agg(F.count("id").alias("total"), F.avg("salary").alias("avg_salary")) \
  .orderBy(F.desc("avg_salary")) \
  .show()

# Overall stats
df.describe("salary", "age").show()
```
```
+-------+--------+-----+
|summary|  salary|  age|
+-------+--------+-----+
|  count|      10|   10|
|   mean| 49800.0| 31.8|
|    min|   39000|   25|
|    max|   61000|   40|
+-------+--------+-----+
```

## Read Directly from Hive
```python
# Query Hive table directly from Spark
spark.sql("""
  SELECT fraud_label,
         COUNT(*) AS cnt,
         ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
  FROM bd_class_project.cc_fraud_trans
  GROUP BY fraud_label
  ORDER BY fraud_label
""").show()
```
```
+-----------+-----+----+
|fraud_label|  cnt| pct|
+-----------+-----+----+
|          0|25113|67.9|
|          1|11887|32.1|
+-----------+-----+----+
```

## Write to HDFS and Hive
```python
# Write as Parquet to HDFS
df_banded.write \
    .mode("overwrite") \
    .parquet("hdfs://ip-172-31-3-251.eu-west-2.compute.internal:8020/tmp/itc_runbook/employees_banded")

# Write as Hive managed table
df_banded.write \
    .mode("overwrite") \
    .format("hive") \
    .saveAsTable("itc_runbook.employees_banded")
```

## Run as a Script (spark-submit)
```python
# /tmp/spark_etl.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("employees_etl").enableHiveSupport().getOrCreate()

df = spark.sql("SELECT * FROM itc_runbook.employees")
result = df.filter(F.col("salary") > 50000) \
           .withColumn("tax", F.col("salary") * 0.20) \
           .select("name", "city", "salary", "tax")

result.write.mode("overwrite").format("hive").saveAsTable("itc_runbook.high_earners")
print(f"Written {result.count()} high-earner rows")
spark.stop()
```

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 1g \
  --num-executors 2 \
  /tmp/spark_etl.py
```

## Window Functions
```python
from pyspark.sql.window import Window

# Rank employees by salary within each city
window = Window.partitionBy("city").orderBy(F.desc("salary"))
df.withColumn("rank", F.rank().over(window)).show()

# Running total of salary
window_cum = Window.orderBy("salary").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("cumulative_salary", F.sum("salary").over(window_cum)).show()
```

## Structured Streaming (Micro-batch)
```python
# Read from streaming HDFS directory (new files trigger processing)
stream_df = spark.readStream \
    .schema(df.schema) \
    .csv("hdfs://ip-172-31-3-251.eu-west-2.compute.internal:8020/tmp/itc_runbook/stream_in/")

query = stream_df.writeStream \
    .format("csv") \
    .option("path", "hdfs://ip-172-31-3-251.eu-west-2.compute.internal:8020/tmp/itc_runbook/stream_out/") \
    .option("checkpointLocation", "/tmp/itc_runbook/checkpoint/") \
    .start()

query.awaitTermination()
```

## Day 7 Exercise
1. Open a PySpark shell on the cluster
2. Read `employees.csv` from HDFS into a DataFrame
3. Add a `senior` column: `True` if age >= 35, else `False`
4. Calculate average salary for senior vs junior employees
5. Write the result as Hive table `itc_runbook.<yourname>_analysis`
6. Verify with `beeline`: `SELECT * FROM itc_runbook.<yourname>_analysis;`

---

# Day 8 — Kafka (Real-Time Streaming)

## What is Kafka?

Apache Kafka is a **distributed streaming platform** — a high-throughput, fault-tolerant message queue. Producers write events to topics; consumers read them independently at their own pace.

```
Kafka Producer (Spark / Python script)
       │
       ▼
Kafka Topic: cc_fraud_stream
  ├── Partition 0  ──► Broker ip-172-31-3-251:9092
  ├── Partition 1  ──► Broker ip-172-31-6-42:9092
  └── Partition 2  ──► Broker ip-172-31-3-85:9092
       │
       ▼
Kafka Consumer (Spark Streaming / Python consumer)
```

## Live Topics on This Cluster
```bash
kafka-topics --list \
  --bootstrap-server ip-172-31-3-251.eu-west-2.compute.internal:9092
```
```
__consumer_offsets
anjan_orders
cc_fraud_stream        ← class project stream (3 partitions)
claims_topic
f1_lap_times
itc_runbook_test
order_stream_topic
test
transaction_topic
user1
watch_history
```

## Inspect a Topic
```bash
kafka-topics --describe \
  --topic cc_fraud_stream \
  --bootstrap-server ip-172-31-3-251.eu-west-2.compute.internal:9092
```
```
Topic: cc_fraud_stream   PartitionCount: 3   ReplicationFactor: 1
  Partition: 0  Leader: 1546338152  Isr: 1546338152
  Partition: 1  Leader: 1546338148  Isr: 1546338148
  Partition: 2  Leader: 1546338156  Isr: 1546338156
```

## Create a Topic
```bash
kafka-topics --create \
  --topic itc_runbook_test \
  --bootstrap-server ip-172-31-3-251.eu-west-2.compute.internal:9092 \
  --partitions 3 \
  --replication-factor 1
```

## Produce Messages (Console)
```bash
kafka-console-producer \
  --topic itc_runbook_test \
  --bootstrap-server ip-172-31-3-251.eu-west-2.compute.internal:9092
```
```
> {"user_id": "USER_001", "amount": 150.00, "fraud": 0}
> {"user_id": "USER_002", "amount": 9500.00, "fraud": 1}
> {"user_id": "USER_003", "amount": 42.50, "fraud": 0}
> ^C
```

## Consume Messages (Console)
```bash
kafka-console-consumer \
  --topic itc_runbook_test \
  --bootstrap-server ip-172-31-3-251.eu-west-2.compute.internal:9092 \
  --from-beginning
```
```
{"user_id": "USER_001", "amount": 150.00, "fraud": 0}
{"user_id": "USER_002", "amount": 9500.00, "fraud": 1}
{"user_id": "USER_003", "amount": 42.50, "fraud": 0}
```

## Python Producer
```python
# /tmp/kafka_producer.py
from kafka import KafkaProducer
import json, time, random

BROKERS = [
    "ip-172-31-3-251.eu-west-2.compute.internal:9092",
    "ip-172-31-6-42.eu-west-2.compute.internal:9092",
    "ip-172-31-3-85.eu-west-2.compute.internal:9092"
]

producer = KafkaProducer(
    bootstrap_servers=BROKERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for i in range(20):
    event = {
        "transaction_id": f"TXN_{90000 + i}",
        "user_id": f"USER_{random.randint(1000, 9999)}",
        "amount": round(random.uniform(10, 5000), 2),
        "fraud_label": random.choice([0, 0, 0, 1])
    }
    producer.send("itc_runbook_test", value=event)
    print(f"Sent: {event}")
    time.sleep(0.5)

producer.flush()
producer.close()
```

```bash
/home/consultant/pyenv/bin/python /tmp/kafka_producer.py
```

## Python Consumer
```python
# /tmp/kafka_consumer.py
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "itc_runbook_test",
    bootstrap_servers=["ip-172-31-3-251.eu-west-2.compute.internal:9092"],
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    group_id="itc_runbook_group"
)

for msg in consumer:
    event = msg.value
    flag = "FRAUD" if event.get("fraud_label") == 1 else "OK"
    print(f"[{flag}] TXN={event['transaction_id']}  USER={event['user_id']}  £{event['amount']}")
```

## Spark Structured Streaming — Kafka to HDFS

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder.appName("KafkaToHDFS").master("yarn").getOrCreate()

schema = StructType([
    StructField("Transaction_ID",     StringType()),
    StructField("User_ID",            StringType()),
    StructField("Transaction_Amount", DoubleType()),
    StructField("Fraud_Label",        IntegerType())
])

BROKERS = "ip-172-31-3-251.eu-west-2.compute.internal:9092,ip-172-31-6-42.eu-west-2.compute.internal:9092"

df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BROKERS) \
    .option("subscribe", "cc_fraud_stream") \
    .option("startingOffsets", "latest") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

query = df_stream.writeStream \
    .format("csv") \
    .option("path", "hdfs://ip-172-31-3-251.eu-west-2.compute.internal:8020/tmp/itc_runbook/kafka_out/") \
    .option("checkpointLocation", "/tmp/kafka_checkpoint/") \
    .start()

query.awaitTermination()
```

## Consumer Groups and Lag
```bash
# List consumer groups
kafka-consumer-groups \
  --bootstrap-server ip-172-31-3-251.eu-west-2.compute.internal:9092 \
  --list

# Check lag
kafka-consumer-groups \
  --bootstrap-server ip-172-31-3-251.eu-west-2.compute.internal:9092 \
  --group itc_runbook_group \
  --describe
```
```
GROUP              TOPIC             PARTITION  OFFSET  LOG-END  LAG
itc_runbook_group  itc_runbook_test  0          23      23       0   ← fully consumed
```

## Delete a Topic
```bash
kafka-topics --delete \
  --topic itc_runbook_test \
  --bootstrap-server ip-172-31-3-251.eu-west-2.compute.internal:9092
```

## Day 8 Exercise
1. Create a Kafka topic: `<yourname>_test` with 3 partitions
2. Write a Python producer that sends 10 employee records as JSON
3. Write a Python consumer that reads them and prints only `salary > 50000`
4. Check consumer lag with `kafka-consumer-groups --describe`
5. Delete the topic when done

---

# Day 9 — HBase (NoSQL on Hadoop)

## What is HBase?

HBase is a **distributed NoSQL database** built on top of HDFS. Unlike Hive's batch SQL, HBase gives **millisecond random read/write access** to individual rows — ideal for real-time lookups.

```
Data model: Table → Row Key → Column Family → Column → Value

Example: cc_fraud_realtime
  Row Key: TXN_12345
    cf:user_id            = USER_6086
    cf:transaction_amount = 72.54
    cf:fraud_label        = 0
```

## Live Tables on This Cluster
```bash
echo 'list' | hbase shell
```
```
TABLE
cc_fraud_realtime    ← 33,000 rows from Kafka consumer
created_users        ← 1,221 user records from Spark pipeline
2 row(s)
```

## HBase Shell Basics

```bash
hbase shell
```

```ruby
# List all tables
list

# Create a table with column family 'cf'
create 'itc_runbook_emp', 'cf'

# Insert rows (put 'table', 'rowkey', 'cf:column', 'value')
put 'itc_runbook_emp', '1', 'cf:name',   'Alice'
put 'itc_runbook_emp', '1', 'cf:city',   'London'
put 'itc_runbook_emp', '1', 'cf:salary', '45000'

put 'itc_runbook_emp', '2', 'cf:name',   'Bob'
put 'itc_runbook_emp', '2', 'cf:city',   'Manchester'
put 'itc_runbook_emp', '2', 'cf:salary', '52000'

# Read a specific row by key — instant lookup
get 'itc_runbook_emp', '1'
```
```
COLUMN                CELL
cf:city               timestamp=1774862595469, value=London
cf:name               timestamp=1774862595464, value=Alice
cf:salary             timestamp=1774862595469, value=45000
```

```ruby
# Scan entire table
scan 'itc_runbook_emp'
```
```
ROW  COLUMN+CELL
1    column=cf:city,   timestamp=..., value=London
1    column=cf:name,   timestamp=..., value=Alice
1    column=cf:salary, timestamp=..., value=45000
2    column=cf:city,   timestamp=..., value=Manchester
2    column=cf:name,   timestamp=..., value=Bob
2    column=cf:salary, timestamp=..., value=52000
2 row(s)
```

```ruby
# Scan with limit
scan 'itc_runbook_emp', {LIMIT => 1}

# Scan specific column only
scan 'itc_runbook_emp', {COLUMNS => ['cf:salary']}

# Count rows
count 'itc_runbook_emp'

# Delete a specific column value
delete 'itc_runbook_emp', '2', 'cf:salary'

# Delete entire row
deleteall 'itc_runbook_emp', '2'

# Drop table (disable first)
disable 'itc_runbook_emp'
drop 'itc_runbook_emp'
```

## Live Data: created_users Table
```ruby
scan 'created_users', {LIMIT => 3}
```
```
ROW                                    COLUMN+CELL
006f79c3-f375-46bf-b909-6baa7b8aae97  cf:email=user5574@test.com
006f79c3-f375-46bf-b909-6baa7b8aae97  cf:first_name=Emma
006f79c3-f375-46bf-b909-6baa7b8aae97  cf:last_name=Smith
006f79c3-f375-46bf-b909-6baa7b8aae97  cf:username=user_06853c38
00b5172d-6372-4e33-9379-e60cfbb282e8  cf:email=user4872@test.com
00b5172d-6372-4e33-9379-e60cfbb282e8  cf:first_name=Sophia
00b5172d-6372-4e33-9379-e60cfbb282e8  cf:last_name=Johnson
010dce60-4230-4273-bb1e-5e31b1b2149f  cf:first_name=John
010dce60-4230-4273-bb1e-5e31b1b2149f  cf:last_name=Brown
3 row(s)
```

## Live Data: cc_fraud_realtime (33,000 rows)
```ruby
count 'cc_fraud_realtime'
```
```
Current count: 33000, row: TXN_9999
33000 row(s)
```

```ruby
describe 'cc_fraud_realtime'
```
```
{NAME => 'cf', COMPRESSION => 'SNAPPY', VERSIONS => '1', TTL => 'FOREVER',
 BLOCKCACHE => 'false', BLOCKSIZE => '65536'}
```

## Write to HBase from PySpark

```python
import subprocess

def write_to_hbase(row_key, columns, table):
    """Write a dict of column values to HBase via hbase shell subprocess."""
    commands = [f"put '{table}', '{row_key}', 'cf:{k}', '{v}'" for k, v in columns.items()]
    commands.append("exit")
    subprocess.run(["hbase", "shell"], input="\n".join(commands).encode(), capture_output=True)

# Write Spark DataFrame rows to HBase
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SparkHBase").master("yarn").enableHiveSupport().getOrCreate()
df = spark.sql("SELECT * FROM itc_runbook.employees")

for row in df.collect():
    write_to_hbase(
        row_key=str(row["id"]),
        columns={"name": row["name"], "city": row["city"], "salary": str(row["salary"])},
        table="itc_runbook_emp"
    )
print("All rows written to HBase")
```

## Day 9 Exercise
1. Create HBase table `<yourname>_orders` with column family `cf`
2. Insert 5 rows: row key = order ID, columns: `cf:product`, `cf:amount`, `cf:status`
3. Use `get` to retrieve a single row by key
4. Use `scan` with `{COLUMNS => ['cf:amount']}` to see only amounts
5. Count rows: `count '<yourname>_orders'`
6. Delete one row and verify it's gone
7. Disable and drop the table

---

# Day 10 — End-to-End Project: CC Fraud Detection Pipeline

## Project Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        CC FRAUD PIPELINE                             │
│                                                                      │
│  PostgreSQL (testdb.cc_fraud_trans)  37,000 rows                     │
│       │                                                              │
│  [1] Sqoop Import ──────────────────────────────────────────────►    │
│       │                                        HDFS: raw_data_sqoop  │
│       │                                                      │       │
│  [2] Hive External Table ◄───────────────────────────────────┘       │
│       bd_class_project.cc_fraud_trans (37,000 rows)                  │
│       │                                                              │
│  [3] Spark Transformations (10 transforms) ──────────────────────►   │
│                               cc_fraud_trans_curated (37,000 rows)   │
│                                                                      │
│  [4] ML Training (RandomForest) ─────────────────────────────────►   │
│       Hive: bd_class_project.ml_from_csv     AUC: 0.9312             │
│                                              model: file:///app/model│
│                                                                      │
│  ─────────── REAL-TIME STREAM ──────────────────────────────────     │
│                                                                      │
│  Kafka Topic: cc_fraud_stream (3 partitions)                         │
│       │                                                              │
│  [5] Spark Structured Streaming (Scala) ─────────────────────────►   │
│       │                               HBase: cc_fraud_realtime       │
│       │                               (33,000 rows, SNAPPY)          │
│       │                                                      │       │
│  [6] Prediction Cron (predict_from_hbase.py) ◄───────────────┘       │
│       │                                                              │
│       ▼                                                              │
│  Hive: bd_class_project.predictions_realtime                         │
│                                                                      │
│  [7] Jenkins CI/CD — auto test + deploy on git push                  │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Step 1 — Raw Layer: PostgreSQL → HDFS via Sqoop

### Populate PostgreSQL with source data
```bash
# On server 13.42.152.118
export DB_HOST=localhost DB_PORT=5432 DB_NAME=testdb
export DB_USERNAME=admin DB_PASSWORD=admin123
python3 ingest_to_postgres.py full
```
```
Connected to PostgreSQL at localhost:5432/testdb
Read 37,000 rows from full_load.csv
Full load complete: table 'cc_fraud_trans' replaced with 37,000 rows.
```

### Import to HDFS
```bash
sqoop import \
  --connect 'jdbc:postgresql://13.42.152.118:5432/testdb' \
  --username admin \
  --password admin123 \
  --table cc_fraud_trans \
  --target-dir /tmp/US_UK_05052025/class_project/input/raw_data_sqoop \
  --num-mappers 4 \
  --fields-terminated-by ',' \
  --delete-target-dir
```
```
INFO mapreduce.ImportJobBase: Transferred 4.22 MB in 38.2 seconds
INFO mapreduce.ImportJobBase: Retrieved 37000 records.
```

```bash
hdfs dfs -ls /tmp/US_UK_05052025/class_project/input/raw_data_sqoop/
```
```
-rwxrwxrwx  3 ec2-user supergroup  4424726  part-m-00000
-rwxrwxrwx  3 ec2-user supergroup   856765  part-m-00001
-rwxrwxrwx  3 ec2-user supergroup        0  part-m-00002
-rwxrwxrwx  3 ec2-user supergroup        0  part-m-00003
```

---

## Step 2 — Raw Hive Table

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS bd_class_project.cc_fraud_trans (
  transaction_id               STRING,
  user_id                      STRING,
  transaction_amount           DECIMAL(10,2),
  transaction_type             STRING,
  timestamp                    TIMESTAMP,
  account_balance              DECIMAL(10,2),
  device_type                  STRING,
  location                     STRING,
  merchant_category            STRING,
  ip_address_flag              INT,
  previous_fraudulent_activity INT,
  daily_transaction_count      INT,
  avg_transaction_amount_7d    DECIMAL(10,2),
  failed_transaction_count_7d  INT,
  card_type                    STRING,
  card_age                     INT,
  transaction_distance         DECIMAL(10,2),
  authentication_method        STRING,
  risk_score                   DECIMAL(5,4),
  is_weekend                   INT,
  fraud_label                  INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/US_UK_05052025/class_project/input/raw_data_sqoop';

SELECT COUNT(*) FROM bd_class_project.cc_fraud_trans;
```
```
+-------+
|  _c0  |
+-------+
| 37000 |
+-------+
```

```sql
SELECT fraud_label, COUNT(*) AS cnt FROM bd_class_project.cc_fraud_trans GROUP BY fraud_label;
```
```
+--------------+-------+
| fraud_label  |  cnt  |
+--------------+-------+
| 0            | 25113 |   ← 67.9% legitimate
| 1            | 11887 |   ← 32.1% fraudulent
+--------------+-------+
```

---

## Step 3 — Curated Layer: Spark Transformations

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --conf spark.sql.hive.convertMetastoreParquet=false \
  ON_PREM/data_ingestion_batch/src/curated_layer/spark/full_load.py
```
```
INFO  curated_full_load  === Curated Full Load – START ===
INFO  curated_full_load  Raw row count: 37000
INFO  curated_full_load  Applying all transformations …
INFO  curated_full_load  Curated row count (after dedup/null handling): 37000
INFO  curated_full_load  Rows now in curated table: 37000
INFO  curated_full_load  === Curated Full Load – END ===
```

### The 10 Transformations
| # | Function | New Column(s) | Logic |
|---|---|---|---|
| 1 | `remove_duplicates` | — | Drop duplicate `transaction_id` |
| 2 | `handle_nulls` | — | Fill missing amounts, drop rows missing critical fields |
| 3 | `extract_time_features` | `txn_hour`, `txn_day_of_week`, `txn_month`, `is_weekend` | Derived from `timestamp` |
| 4 | `normalize_amount` | `amt_log` | `log1p(transaction_amount)` |
| 5 | `bucket_amount` | `amt_bucket` | low / medium / high / very_high |
| 6 | `calculate_age` | `cardholder_age` | Cast `card_age` INT |
| 7 | `calculate_distance` | `distance_km` | Alias `transaction_distance` |
| 8 | `encode_gender` | `gender_encoded` | Biometric auth = 1, else 0 |
| 9 | `transaction_velocity` | `txn_velocity_day` | Count daily txns per user |
| 10 | `flag_high_risk` | `high_risk` | amount > 500 AND hour 0–5 AND velocity > 3 |

### Daily Incremental Run
```bash
# Only processes rows newer than MAX(Timestamp) in curated table
spark-submit \
  --master yarn \
  --deploy-mode client \
  --conf spark.sql.hive.convertMetastoreParquet=false \
  ON_PREM/data_ingestion_batch/src/curated_layer/spark/incremental_load.py
```
```
INFO  Watermark from curated table: 2024-12-31 23:59:58
INFO  New raw rows to process: 0
INFO  No new data since watermark – exiting.
```

---

## Step 4 — ML Model Training (RandomForest)

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --conf "spark.yarn.appMasterEnv.HIVE_METASTORE_URIS=thrift://172.31.6.42:9083" \
  ON_PREM/ml/create_model/train_rf_from_hive.py
```
```
Reading from bd_class_project.ml_from_csv ...
Training RandomForest (100 trees, maxDepth=5) ...
Test AUC = 0.9312
Model saved to file:///app/model
```

**Model details:**
- Algorithm: RandomForest — 100 trees, depth 5, seed 42
- Input: 14 numeric features (amounts, counts, flags, distances, risk score)
- Target: `fraud_label` (0 = legitimate, 1 = fraudulent)
- AUC = 0.9312 — strong fraud detection
- 70/30 train/test split

---

## Step 5 — Real-Time Stream: Kafka → HBase

The Scala consumer (`kafkaconsumer.scala`) runs as a Spark Structured Streaming job:

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class kafkaconsumer \
  kafka-streaming.jar
```

Each Kafka message from `cc_fraud_stream` → HBase row key = `Transaction_ID`, column family = `cf`.

```bash
echo 'count "cc_fraud_realtime"' | hbase shell
```
```
Current count: 33000, row: TXN_9999
33000 row(s)
```

---

## Step 6 — Real-Time Predictions: HBase → ML → Hive

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --conf "spark.yarn.appMasterEnv.HBASE_HOST=172.31.6.42" \
  --conf "spark.yarn.appMasterEnv.MODEL_PATH=file:///app/model" \
  ON_PREM/ml/predict/predict_from_hbase.py
```

Steps:
1. Reads new rows from `cc_fraud_realtime` HBase
2. Applies the trained RandomForest `PipelineModel`
3. Appends predictions to `bd_class_project.predictions_realtime` Hive table

---

## Step 7 — Jenkins CI/CD Pipeline

### GitHub → Jenkins → YARN

Every `git push` to https://github.com/uttamraj9/Test_Spark_Jenkins triggers:

```
git push
   │
   ▼ GitHub Webhook (POST to :8080/github-webhook/)
Jenkins (http://13.42.152.118:8080)
   │
   ├── Stage: Checkout code
   ├── Stage: Setup Python venv + pip install
   ├── Stage: Run pytest --junitxml=pytest.xml
   ├── Stage: Publish JUnit test results
   └── (on success) SCP + spark-submit on 13.41.167.97
```

```groovy
// Jenkinsfile
pipeline {
  agent any
  environment { VENV = 'unit_testing_bd' }

  stages {
    stage('Checkout') { steps { checkout scm } }

    stage('Setup Python') {
      steps {
        sh '''
          python3 -m venv ${VENV}
          source ${VENV}/bin/activate
          pip install -r requirements.txt
        '''
      }
    }

    stage('Unit Tests') {
      steps {
        sh '''
          source ${VENV}/bin/activate
          pytest --junitxml=pytest.xml
        '''
      }
    }

    stage('Publish Results') { steps { junit 'pytest.xml' } }
  }

  post {
    success { echo 'All tests passed!' }
    failure { echo 'Tests failed — deployment blocked.' }
    always  { deleteDir() }
  }
}
```

### Set Up a New Pipeline in Jenkins
1. Open http://13.42.152.118:8080 → Login: `consultants / WelcomeItc@2022`
2. **New Item** → **Pipeline** → Name it → OK
3. **Pipeline** → **Definition**: `Pipeline script from SCM`
4. Set repo URL: `https://github.com/uttamraj9/Test_Spark_Jenkins`
5. **Build Triggers** → check `GitHub hook trigger for GITScm polling`
6. In GitHub: **Settings → Webhooks → Add**: `http://13.42.152.118:8080/github-webhook/`
7. Every `git push` now triggers tests → deploy automatically

---

## Capstone Challenge — Build It Yourself

Try building the full pipeline without looking at the steps above:

1. **Ingest**: Sqoop import `cc_fraud_trans` from PostgreSQL to HDFS at `/tmp/<yourname>/raw`
2. **Raw layer**: Create a Hive external table `<yourname>_db.cc_fraud_raw` pointing at that path
3. **Curate**: Write a PySpark script applying at least 5 transforms, write to `<yourname>_db.cc_fraud_curated`
4. **Analyse**: In Impala, after `INVALIDATE METADATA`, find top 5 merchant categories by fraud rate
5. **Stream**: Push 5 fake JSON transactions via Kafka console producer to `itc_runbook_test`
6. **Consume**: Python consumer that prints `FRAUD` / `OK` per transaction
7. **Store**: Write those 5 rows to HBase table `<yourname>_realtime` with column family `cf`
8. **Automate**: Jenkins pipeline that runs `pytest` against your transform functions on every push

---

## Quick Troubleshooting Reference

| Symptom | Cause | Fix |
|---|---|---|
| `Permission denied` on HDFS put | Wrong user/group | `sudo -u hdfs hdfs dfs -chmod 777 /path` |
| `LOAD DATA INPATH` fails | `hive` user can't write | `sudo -u hdfs hdfs dfs -chown hive:hive /path` |
| Beeline hangs on connect | HiveServer2 not ready | Wait 30s, retry; check Cloudera Manager |
| Spark `PATH_NOT_FOUND` | Relative HDFS path | Use `hdfs://ip-172-31-3-251.eu-west-2.compute.internal:8020/path` |
| MapReduce output dir exists | Previous run left output | `hdfs dfs -rm -r /tmp/output` then rerun |
| Kafka consumer no messages | Wrong offset setting | Add `--from-beginning` or `auto.offset.reset=earliest` |
| HBase `disable` fails | Table has open scanners | Wait a few seconds and retry |
| YARN job stuck | All nodes busy | `yarn application -kill <app-id>` and resubmit |
| Sqoop connection refused | Using `localhost` from Cloudera server | Use `jdbc:postgresql://13.42.152.118:5432/testdb` |
| Impala `AnalysisException` | Stale metadata after Hive write | `INVALIDATE METADATA <table>` in Impala shell |
| `spark.sql.extensions` static config error | Already set in cluster; Delta is built in | Remove those 3 `spark.conf.set` lines |

---

## Service Summary

| Service | Version | CLI / Shell | Web UI | Purpose |
|---|---|---|---|---|
| HDFS | Hadoop 3.1.1 | `hdfs dfs ...` | CM → HDFS | Distributed file storage |
| YARN | 3.1.1 | `yarn application ...` | :8088 | Cluster resource management |
| MapReduce | 3.1.1 | `hadoop jar ...` | YARN UI | Batch parallel processing |
| Sqoop | 1.4.7 | `sqoop import ...` | — | RDBMS ↔ Hadoop data transfer |
| Hive | CDH 7.1.7 | `beeline` | Hue :8888 | SQL-on-Hadoop batch analytics |
| Impala | CDH 7.1.7 | `impala-shell` | Hue :8888 | Fast interactive SQL |
| Spark | 3.2.4 | `spark-submit` / `pyspark` | Spark History :18088 | In-memory distributed processing |
| Kafka | CDH 7.1.7 | `kafka-topics ...` | CM | Real-time event streaming |
| HBase | 2.2.3 | `hbase shell` | CM | NoSQL real-time random access |
| Jenkins | 2.x | — | :8080 | CI/CD pipeline automation |
| Cloudera Manager | 7.1.7 | — | :7180 | Cluster management & monitoring |
| Hue | CDH 7.1.7 | — | :8888 | Web UI for SQL, HDFS, Jobs |
