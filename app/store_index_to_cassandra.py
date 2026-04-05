from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
import time

# read index from hdfs and load to cassandra

def check_cassandra(host='127.0.0.1', port=9042, wait=120):
    t0 = time.time()
    while time.time() - t0 < wait:
        try:
            c = Cluster([host], port=port)
            s = c.connect()
            s.shutdown()
            c.shutdown()
            print("cassandra ok")
            return
        except Exception as e:
            print(f"waiting cassandra - {e}")
            time.sleep(5)
    raise Exception("cassandra timeout")


def setup_tables():
    c = Cluster(['127.0.0.1'], port=9042)
    s = c.connect()

    s.execute("""
        CREATE KEYSPACE IF NOT EXISTS search_engine
        WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}
    """)
    s.set_keyspace('search_engine')

    # drop old tables
    s.execute("DROP TABLE IF EXISTS inverted_index")
    s.execute("DROP TABLE IF EXISTS doc_stats")
    s.execute("DROP TABLE IF EXISTS corpus_stats")

    s.execute("""
        CREATE TABLE inverted_index (
            term text, doc_id text,
            tf int, df int, doc_length int,
            PRIMARY KEY (term, doc_id)
        )
    """)
    s.execute("""
        CREATE TABLE doc_stats (
            doc_id text PRIMARY KEY,
            doc_title text, doc_length int
        )
    """)
    s.execute("""
        CREATE TABLE corpus_stats (
            id int PRIMARY KEY,
            num_docs int, avg_dl double, total_dl bigint
        )
    """)
    s.shutdown()
    c.shutdown()
    print("tables ready")


def load_data():
    spark = SparkSession.builder \
        .appName("load_to_cassandra") \
        .master("local[*]") \
        .getOrCreate()
    sc = spark.sparkContext

    # read inverted index
    print("reading inverted index")
    rdd = sc.textFile("hdfs://master:9000/indexer/inverted_index/part-*")

    def parse_index(line):
        cols = line.split("\t")
        if len(cols) < 3:
            return []
        term = cols[0]
        df = int(cols[1])
        res = []
        for p in cols[2].split(","):
            pp = p.split(":")
            if len(pp) >= 3:
                res.append((term, pp[0], int(pp[1]), df, int(pp[2])))
        return res

    idx = rdd.flatMap(parse_index).collect()
    print(f"got {len(idx)} index entries")

    # read doc stats
    print("reading doc stats")
    ds_rdd = sc.textFile("hdfs://master:9000/indexer/doc_stats/part-*")

    doc_list = []
    stats = None
    for line in ds_rdd.collect():
        cols = line.split("\t")
        if cols[0] == "DOC" and len(cols) >= 4:
            doc_list.append((cols[1], cols[2], int(cols[3])))
        elif cols[0] == "STATS" and len(cols) >= 4:
            stats = (int(cols[1]), float(cols[2]), int(float(cols[3])))

    print(f"docs: {len(doc_list)}, N={stats[0]}, avgdl={stats[1]:.1f}")

    # insert to cassandra
    c = Cluster(['127.0.0.1'], port=9042)
    s = c.connect('search_engine')

    # inverted index
    stmt = s.prepare(
        "INSERT INTO inverted_index (term,doc_id,tf,df,doc_length) VALUES (?,?,?,?,?)"
    )
    for i, row in enumerate(idx):
        s.execute(stmt, row)
        if i % 10000 == 0:
            print(f"  index: {i}/{len(idx)}")
    print(f"inserted {len(idx)} index rows")

    # doc stats
    ds = s.prepare("INSERT INTO doc_stats (doc_id,doc_title,doc_length) VALUES (?,?,?)")
    for row in doc_list:
        s.execute(ds, row)
    print(f"inserted {len(doc_list)} doc stats")

    # corpus stats
    if stats:
        s.execute(
            "INSERT INTO corpus_stats (id,num_docs,avg_dl,total_dl) VALUES (%s,%s,%s,%s)",
            (1, stats[0], stats[1], stats[2])
        )
    print("inserted corpus stats")

    s.shutdown()
    c.shutdown()
    spark.stop()
    print("loading done")


if __name__ == "__main__":
    check_cassandra()
    setup_tables()
    load_data()
