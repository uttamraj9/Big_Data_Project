# HDFS & MapReduce — Lab Notes
# Day 1 | Trainer: run all commands in Linux Shell lab (Wetty terminal on edge node 13.41.167.97)

---

## Lab 1 — Browse the Cluster

```bash
# Check your HDFS home directory
hdfs dfs -ls /user/consultant/

# Explore the cluster root
hdfs dfs -ls /
hdfs dfs -ls /user/hive/warehouse/

# Disk usage
hdfs dfs -du -h /user/
```

---

## Lab 2 — Create Directories & Upload Data

```bash
# Create lab directory structure
hdfs dfs -mkdir -p /user/consultant/labs/input
hdfs dfs -mkdir -p /user/consultant/labs/output

# Generate a sample CSV locally (100k rows)
python3 -c "
import csv, random, sys
w = csv.writer(sys.stdout)
w.writerow(['id','amount','category','is_fraud'])
cats = ['grocery','travel','online','restaurant']
for i in range(100000):
    w.writerow([i, round(random.uniform(10,5000),2),
                random.choice(cats), random.randint(0,1)])
" > /tmp/transactions.csv

wc -l /tmp/transactions.csv   # verify: should be 100001 lines

# Upload to HDFS
hdfs dfs -put /tmp/transactions.csv /user/consultant/labs/input/

# Verify
hdfs dfs -ls -lh /user/consultant/labs/input/
```

---

## Lab 3 — Read & Inspect Files

```bash
# Read first 5 lines (cat + head)
hdfs dfs -cat /user/consultant/labs/input/transactions.csv | head -5

# Read last 1 KB
hdfs dfs -tail /user/consultant/labs/input/transactions.csv

# File stats (size, replication, block size)
hdfs dfs -stat "%n: %b bytes, replication=%r, blocksize=%o" \
  /user/consultant/labs/input/transactions.csv
```

---

## Lab 4 — Copy, Move, Delete

```bash
# Copy within HDFS
hdfs dfs -cp /user/consultant/labs/input/transactions.csv \
             /user/consultant/labs/input/transactions_backup.csv

# Move to tmp
hdfs dfs -mv /user/consultant/labs/input/transactions_backup.csv \
             /user/consultant/labs/

# Delete single file
hdfs dfs -rm /user/consultant/labs/transactions_backup.csv

# Delete directory (recursive)
hdfs dfs -rm -r /user/consultant/labs/output
```

---

## Lab 5 — Run MapReduce WordCount

```bash
# Create a text file
echo "big data hadoop hdfs mapreduce yarn hadoop big data big" \
  > /tmp/words.txt

# Upload
hdfs dfs -put /tmp/words.txt /user/consultant/labs/input/

# Run WordCount from the CDH examples JAR
yarn jar \
  /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar \
  wordcount \
  /user/consultant/labs/input/words.txt \
  /user/consultant/labs/output/wordcount

# Watch progress in YARN UI: http://13.42.152.118:30088

# Read output
hdfs dfs -cat /user/consultant/labs/output/wordcount/part-r-00000

# Expected output:
# big     3
# data    2
# hadoop  2
# ...
```

---

## Lab 6 — Cluster Health Check

```bash
# All DataNodes, capacity, usage
hdfs dfsadmin -report

# Quick summary
hdfs dfsadmin -report | grep -E "Live|Dead|Configured Capacity|DFS Used"

# Check safe mode status (should be OFF during normal operation)
hdfs dfsadmin -safemode get

# Block health check for your directory
hdfs fsck /user/consultant/labs -files -blocks | tail -10
# Should show: Status: HEALTHY, Under replicated blocks: 0
```

---

## Lab 7 — Download Back to Local

```bash
# Download single file
hdfs dfs -get /user/consultant/labs/input/transactions.csv \
              /tmp/from_hdfs.csv

# Merge multiple part files into one local file
hdfs dfs -getmerge /user/consultant/labs/output/wordcount/ \
                   /tmp/wordcount_merged.txt

cat /tmp/wordcount_merged.txt
```

---

## HDFS UI

Open in browser: **http://13.42.152.118:30870**

- Utilities → Browse the file system → navigate to /user/consultant/
- Datanodes → check all 3 nodes are Live
- Summary → check Under-replicated Blocks = 0

---

## Troubleshooting

| Problem | Command |
|---------|---------|
| Permission denied | `hdfs dfs -chmod 755 /user/consultant/labs/` |
| Output dir exists (MapReduce fails) | `hdfs dfs -rm -r /user/consultant/labs/output/wordcount` |
| Cluster in safe mode | `hdfs dfsadmin -safemode leave` |
| Can't find JAR | `find /opt/cloudera/parcels -name "*examples*"` |
| Check YARN job logs | `yarn logs -applicationId application_XXXXX_XXXXX` |
