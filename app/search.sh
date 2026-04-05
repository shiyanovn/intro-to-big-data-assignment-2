if [ -z "$1" ]; then
    echo "usage - search.sh <query>"
    exit 1
fi

QUERY="$*"
echo "searching: $QUERY"

spark-submit \
    --master local[*] \
    /app/query.py "$QUERY"
