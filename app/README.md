## app folder
This folder contains the data folder and all scripts and source code that are required to run your simple search engine. 

### data
This folder stores the text documents required to index. Here you can find a sample of 100 documents from `a.parquet` file from the original source.

### mapreduce
This folder stores the mapper `mapperx.py` and reducer `reducerx.py` scripts for the MapReduce pipelines.

- `mapper1.py` + `reducer1.py`: create postings and document statistics.
- `mapper2.py` + `reducer2.py`: create vocabulary DF and corpus statistics.

### app.py
This is a Python file to write code to store index data in Cassandra.

### app.sh
The entrypoint for the executables in your repository and includes all commands that will run your programs in this folder.

### create_index.sh
A script to create index data using MapReduce pipelines and store them in HDFS under:

- `/indexer/index`
- `/indexer/documents`
- `/indexer/vocabulary`
- `/indexer/stats`

### index.sh
A script to run the MapReduce pipelines and then store data in Cassandra/ScyllaDB.

### prepare_data.py
The script that creates **100** plain-text documents from a parquet file using PySpark.

### prepare_data.sh
Runs data preparation end-to-end:
- upload parquet to HDFS
- create documents and store in HDFS `/data`
- use PySpark RDD to generate one-partition `/input/data` with `<doc_id>\t<doc_title>\t<doc_text>`

### prepare_input_rdd.py
PySpark RDD app that reads documents from HDFS `/data` and writes the indexer input in `/input/data`.

### query.py
A PySpark app that processes a user query and retrieves the top 10 relevant documents ranked with BM25.

### requirements.txt
This file contains all Python depenedencies that are needed for running the programs in this repository. This file is read by pip when installing the dependencies in `app.sh` script.

### search.sh
This script will be responsible for running the `query.py` PySpark app on Hadoop YARN cluster.


### start-services.sh
This script will initiate the services required to run Hadoop components. This script is called in `app.sh` file.


### store_index.sh
This script creates Cassandra/ScyllaDB tables and loads index data from HDFS into them.
