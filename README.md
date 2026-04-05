# big-data-assignment2

# How to run
## Step 1: Install prerequisites
- Docker
- Docker compose
## Step 2: Run the command
```bash
docker compose up 
```
This will create 3 containers, a master node and a worker node for Hadoop, and Cassandra server. The master node will run the script `app/app.sh` as an entrypoint.

## Implemented assignment flow
1. `prepare_data.sh`:
   - reads one parquet file with PySpark and creates **100** plain-text documents
   - stores documents in HDFS `/data` as `<doc_id>_<doc_title>.txt`
   - uses PySpark RDD to read `/data` and writes one-partition `/input/data` with `<doc_id>\t<doc_title>\t<doc_text>`
2. `create_index.sh`: runs 2 Hadoop Streaming pipelines to build:
   - postings in `/indexer/index`
   - document stats in `/indexer/documents`
   - vocabulary (DF) in `/indexer/vocabulary`
   - corpus stats (N, AVGDL) in `/indexer/stats`
3. `store_index.sh`: loads `/indexer/*` datasets into Cassandra keyspace `search_engine`.
4. `index.sh`: runs `create_index.sh` then `store_index.sh`.
5. `search.sh "query text"`: runs `query.py` on YARN and prints top-10 BM25 results (doc_id, title, score).
