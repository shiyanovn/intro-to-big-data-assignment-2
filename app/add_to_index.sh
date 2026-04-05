set -e

if [ -z "$1" ]; then
    echo "usage - add_to_index.sh <path_to_file>"
    exit 1
fi

FILE="$1"
if [ ! -f "$FILE" ]; then
    echo "file not found: $FILE"
    exit 1
fi

FNAME=$(basename "$FILE")
echo "adding document: $FNAME"

# upload file to hdfs /data
hdfs dfs -put -f "$FILE" /data/

# get doc_id and title from filename
DOC_ID=$(echo "$FNAME" | cut -d'_' -f1)
DOC_TITLE=$(echo "$FNAME" | sed "s/^${DOC_ID}_//" | sed 's/\.txt$//')
DOC_TEXT=$(cat "$FILE" | tr '\t' ' ' | tr '\n' ' ' | tr '\r' ' ' | sed 's/  */ /g')

echo "doc_id=$DOC_ID title=$DOC_TITLE"

# create temporary input file for this one doc
TMP_DIR="/tmp/add_doc_input"
rm -rf "$TMP_DIR"
mkdir -p "$TMP_DIR"
echo -e "${DOC_ID}\t${DOC_TITLE}\t${DOC_TEXT}" > "$TMP_DIR/doc.txt"

# also add to /input/data (append)
hdfs dfs -appendToFile "$TMP_DIR/doc.txt" /input/data/part-00000 2>/dev/null || {
    # if append fails put like separate file
    hdfs dfs -put "$TMP_DIR/doc.txt" /input/data/new_${DOC_ID}.txt
}

# put temp input to hdfs for mapreduce
hdfs dfs -rm -r -f /tmp/add_doc
hdfs dfs -mkdir -p /tmp/add_doc
hdfs dfs -put "$TMP_DIR/doc.txt" /tmp/add_doc/

# run mapreduce on just this doc to get index data
hdfs dfs -rm -r -f /tmp/add_doc_idx
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    -D mapreduce.job.reduces=1 \
    -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -input /tmp/add_doc \
    -output /tmp/add_doc_idx

# now use python to update cassandra with the new doc data
python3 /app/add_doc_to_cassandra.py "$DOC_ID" "$DOC_TITLE" "$TMP_DIR/doc.txt"

echo "document $FNAME added"
rm -rf "$TMP_DIR"
