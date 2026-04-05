FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV SPARK_HOME=/opt/spark
ENV CASSANDRA_HOME=/opt/cassandra
ENV PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$CASSANDRA_HOME/bin:$JAVA_HOME/bin:$PATH
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root

RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    ssh \
    rsync \
    curl \
    wget \
    python3 \
    python3-pip \
    net-tools \
    nano \
    procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# install Hadoop
RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz && \
    tar -xzf hadoop-3.3.6.tar.gz -C /opt/ && \
    mv /opt/hadoop-3.3.6 /opt/hadoop && \
    rm hadoop-3.3.6.tar.gz

# install Spark
RUN wget -q https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.0-bin-hadoop3.tgz -C /opt/ && \
    mv /opt/spark-3.5.0-bin-hadoop3 /opt/spark && \
    rm spark-3.5.0-bin-hadoop3.tgz

# install cassandra
RUN wget -q https://archive.apache.org/dist/cassandra/4.1.3/apache-cassandra-4.1.3-bin.tar.gz && \
    tar -xzf apache-cassandra-4.1.3-bin.tar.gz -C /opt/ && \
    mv /opt/apache-cassandra-4.1.3 /opt/cassandra && \
    rm apache-cassandra-4.1.3-bin.tar.gz

# ssh config
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 600 ~/.ssh/authorized_keys && \
    echo "Host *\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n  LogLevel ERROR" > ~/.ssh/config

# Hadoop env
RUN echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# core-site.xml
RUN cat > $HADOOP_CONF_DIR/core-site.xml << 'XMLEOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://master:9000</value>
  </property>
</configuration>
XMLEOF

# hdfs-site.xml
RUN cat > $HADOOP_CONF_DIR/hdfs-site.xml << 'XMLEOF'
<?xml version="1.0"?>
<configuration>
  <property><name>dfs.replication</name><value>2</value></property>
  <property><name>dfs.namenode.name.dir</name><value>/hadoop/dfs/name</value></property>
  <property><name>dfs.datanode.data.dir</name><value>/hadoop/dfs/data</value></property>
  <property><name>dfs.permissions.enabled</name><value>false</value></property>
</configuration>
XMLEOF

# mapred-site.xml
RUN cat > $HADOOP_CONF_DIR/mapred-site.xml << 'XMLEOF'
<?xml version="1.0"?>
<configuration>
  <property><name>mapreduce.framework.name</name><value>yarn</value></property>
  <property><name>mapreduce.application.classpath</name><value>/opt/hadoop/share/hadoop/mapreduce/*:/opt/hadoop/share/hadoop/mapreduce/lib/*</value></property>
  <property><name>yarn.app.mapreduce.am.env</name><value>HADOOP_MAPRED_HOME=/opt/hadoop</value></property>
  <property><name>mapreduce.map.env</name><value>HADOOP_MAPRED_HOME=/opt/hadoop</value></property>
  <property><name>mapreduce.reduce.env</name><value>HADOOP_MAPRED_HOME=/opt/hadoop</value></property>
  <property><name>mapreduce.map.memory.mb</name><value>1024</value></property>
  <property><name>mapreduce.reduce.memory.mb</name><value>1024</value></property>
</configuration>
XMLEOF

# yarn-site.xml
RUN cat > $HADOOP_CONF_DIR/yarn-site.xml << 'XMLEOF'
<?xml version="1.0"?>
<configuration>
  <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
  <property><name>yarn.resourcemanager.hostname</name><value>master</value></property>
  <property><name>yarn.nodemanager.resource.memory-mb</name><value>4096</value></property>
  <property><name>yarn.scheduler.maximum-allocation-mb</name><value>4096</value></property>
  <property><name>yarn.nodemanager.vmem-check-enabled</name><value>false</value></property>
  <property><name>yarn.nodemanager.pmem-check-enabled</name><value>false</value></property>
</configuration>
XMLEOF

# workers
RUN echo "master" > $HADOOP_CONF_DIR/workers && echo "slave1" >> $HADOOP_CONF_DIR/workers

# spark config
RUN cp $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.master yarn" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.submit.deployMode client" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.defaultFS hdfs://master:9000" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.driver.memory 1g" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.executor.memory 1g" >> $SPARK_HOME/conf/spark-defaults.conf

RUN echo "export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop" > $SPARK_HOME/conf/spark-env.sh && \
    echo "export YARN_CONF_DIR=/opt/hadoop/etc/hadoop" >> $SPARK_HOME/conf/spark-env.sh && \
    echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64" >> $SPARK_HOME/conf/spark-env.sh

# cassandra config
RUN sed -i "s/listen_address: localhost/listen_address: master/g" $CASSANDRA_HOME/conf/cassandra.yaml && \
    sed -i "s/rpc_address: localhost/rpc_address: 0.0.0.0/g" $CASSANDRA_HOME/conf/cassandra.yaml && \
    sed -i '/^# broadcast_address:/a broadcast_address: master' $CASSANDRA_HOME/conf/cassandra.yaml && \
    sed -i 's/- seeds: "127.0.0.1:7000"/- seeds: "master"/g' $CASSANDRA_HOME/conf/cassandra.yaml && \
    sed -i 's/- seeds: "127.0.0.1"/- seeds: "master"/g' $CASSANDRA_HOME/conf/cassandra.yaml && \
    sed -i 's/^# broadcast_rpc_address:.*//' $CASSANDRA_HOME/conf/cassandra.yaml && \
    sed -i '/^rpc_address:/a broadcast_rpc_address: master' $CASSANDRA_HOME/conf/cassandra.yaml
# python packages
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# create dirs
RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data /data

WORKDIR /app
