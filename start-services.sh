set -e

echo "start ssh"
service ssh start

if [ "$NODE_TYPE" = "master" ]; then

    if [ ! -d /hadoop/dfs/name/current ]; then
        echo "formatting HDFS namenode"
        hdfs namenode -format -force
    fi

    echo "waiting for slave1"
    for i in $(seq 1 30); do
        if ssh -o ConnectTimeout=2 slave1 echo "slave1 ok" 2>/dev/null; then
            break
        fi
        echo "slave1 not ready - $i"
        sleep 2
    done

    echo "starting HDFS"
    start-dfs.sh

    echo "starting YARN"
    start-yarn.sh

    echo "starting cassandra"
    cassandra -R
    echo "waiting for cassandra"
    for i in $(seq 1 60); do
        if cqlsh localhost -e "DESCRIBE KEYSPACES" 2>/dev/null; then
            echo "cassandra started"
            break
        fi
        echo "cassandra not ready - $i"
        sleep 5
    done

    hdfs dfs -mkdir -p /data
    hdfs dfs -mkdir -p /input/data
    hdfs dfs -mkdir -p /indexer
    hdfs dfs -mkdir -p /tmp/indexer

    echo "master services ready"

elif [ "$NODE_TYPE" = "slave" ]; then
    echo "ssh started, wait for master to start HDFS and YARN"
    sleep infinity &
    wait
fi
