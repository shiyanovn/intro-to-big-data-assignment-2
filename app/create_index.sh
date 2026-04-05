set -e

INPUT=${1:-/input/data}

# cleanup
hdfs dfs -rm -r -f /tmp/indexer
hdfs dfs -rm -r -f /indexer/index
hdfs dfs -rm -r -f /indexer/doc_stats
hdfs dfs -rm -r -f /indexer/inverted_index
hdfs dfs -mkdir -p /tmp/indexer
hdfs dfs -mkdir -p /indexer

echo "pipeline 1 - tf and df"
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -input "$INPUT" \
    -output /tmp/indexer/pipeline1

echo "pipeline 2 - doc stats"
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    -D mapreduce.job.reduces=1 \
    -files /app/mapreduce/mapper2.py,/app/mapreduce/reducer2.py \
    -mapper "python3 mapper2.py" \
    -reducer "python3 reducer2.py" \
    -input "$INPUT" \
    -output /indexer/doc_stats

echo "pipeline 3 - inverted index"
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    -files /app/mapreduce/mapper3.py,/app/mapreduce/reducer3.py \
    -mapper "python3 mapper3.py" \
    -reducer "python3 reducer3.py" \
    -input /tmp/indexer/pipeline1 \
    -output /indexer/inverted_index

# keep full index copy too
hdfs dfs -cp /tmp/indexer/pipeline1 /indexer/index

echo "indexing done"
hdfs dfs -cat /indexer/inverted_index/part-00000 2>/dev/null | head -3
hdfs dfs -cat /indexer/doc_stats/part-00000 2>/dev/null | head -3
