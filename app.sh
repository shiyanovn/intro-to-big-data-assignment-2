set -e

echo "waiting for services"
sleep 5

echo "check hdfs"
for i in $(seq 1 30); do
    if hdfs dfs -ls / 2>/dev/null; then
        echo "hdfs ok"
        break
    fi
    echo "hdfs not ready, attempt $i"
    sleep 3
done

echo "check cassandra"
CASS_OK=false
for i in $(seq 1 60); do
    if cqlsh localhost -e "DESCRIBE KEYSPACES" 2>/dev/null; then
        echo "cassandra ok"
        CASS_OK=true
        break
    fi
    echo "cassandra not ready, attempt $i"
    sleep 5
done

if [ "$CASS_OK" = false ]; then
    echo "cassandra failed"
    exit 1
fi

echo "installing dependencies"
pip3 install -r /requirements.txt 2>/dev/null || true

# download and prepare data
echo "data preparation"

# check if parquet file exists
if [ ! -f /data/a.parquet ]; then
    echo "no parquet file, put a.parquet in ./data/ folder"
    if hdfs dfs -test -e /input/data/part-00000 2>/dev/null; then
        echo "using existing data in hdfs"
    else
        echo "no data"
        exit 1
    fi
else
    echo "preparing 100 documents"
    python3 /app/prepare_data.py /data/a.parquet 100
fi

echo "hdfs /data:"
hdfs dfs -ls /data/ | head -5
echo "files count:"
hdfs dfs -ls /data/ | wc -l

echo "sample of /input/data:"
hdfs dfs -cat /input/data/part-00000 2>/dev/null | head -2 || \
hdfs dfs -cat /input/data/part-* 2>/dev/null | head -2

echo "indexing"
bash /app/index.sh

echo "search queries"

echo "query 1 - history of art"
bash /app/search.sh "history of art"

echo "query 2 - computer science algorithms"
bash /app/search.sh "computer science algorithms"

echo "query 3 - ancient civilization"
bash /app/search.sh "ancient civilization"

echo "all done"
