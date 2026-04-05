set -e

INPUT=${1:-/input/data}

echo "creating index"
bash /app/create_index.sh "$INPUT"

echo "storing to cassandra"
bash /app/store_index.sh

echo "index done"
