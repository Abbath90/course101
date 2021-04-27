## course101
Tasks from bigdata course101.

### hw3
Convert data from csv-file into Parquet and save as a copy.

Usage:
>cd hw3/data_converting
>data_converting>data_converting_into_parquet.py --path_to_file <path_to_file>  - for converting
>data_converting>data_converting_into_parquet.py --help - for help


### hw8
Implement simple daemon that loads booking event (same schema as dataset) into kafka topic. Daemon should generate events in parallel (create parameter to set parallelism degree).
Create Spark Job that reads Kafka topic and put data into HDFS.
Measure the results and performance, compare them with batching.

Usage on hdp:
>/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --zookeeper 172.18.0.2:2181 --topic hotels --partitions 1  --replication-factor 1 --create 

'Topic creation'

>export HADOOP_USER_NAME=hdfs 

'Eliminate permissions problems.'

>cd hw8

>spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 --master local[\*] --deploy-mode client consumer.py 

'Start streaming consumer'

>python producer.py --threads <number of threads> --events <number of events> 

'Start producer. Make sure that you use python3'
  
>spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 --master local[\*] --deploy-mode client consumer_batch.py 

'Start batching consumer'
