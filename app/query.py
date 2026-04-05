import sys
import re
import math
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster

# bm25 search - read query, compute scores from cassandra index, show top 10

STOP = {'the','a','an','is','are','was','were','be','been','being',
    'have','has','had','do','does','did','will','would','could',
    'should','may','might','shall','can','need','dare','ought',
    'used','to','of','in','for','on','with','at','by','from',
    'as','into','through','during','before','after','above','below',
    'between','out','off','over','under','again','further','then',
    'once','here','there','when','where','why','how','all','each',
    'every','both','few','more','most','other','some','such','no',
    'nor','not','only','own','same','so','than','too','very',
    'and','but','or','if','while','because','until','that',
    'which','who','whom','this','these','those','it','its',
    'he','she','they','them','his','her','their','my','your','our',
    'i','me','we','you','what','about','up','also','just'}


def tokenize(text):
    words = re.findall(r'[a-z0-9]+', text.lower())
    return [w for w in words if w not in STOP and len(w) > 1]


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage - query.py <query text>")
        sys.exit(1)

    q = " ".join(sys.argv[1:])
    terms = tokenize(q)
    if not terms:
        print("no terms after tokenization")
        sys.exit(0)

    print(f"query: '{q}'")
    print(f"terms: {terms}")

    # bm25 params
    K1 = 1.5
    B = 0.75

    spark = SparkSession.builder \
        .appName("search") \
        .master("local[*]") \
        .getOrCreate()
    sc = spark.sparkContext

    # connect to cassandra
    cl = Cluster(['127.0.0.1'], port=9042)
    sess = cl.connect('search_engine')

    # get N and avgdl
    r = sess.execute("SELECT num_docs, avg_dl FROM corpus_stats WHERE id=1").one()
    if not r:
        print("no corpus stats")
        sys.exit(1)
    N = r.num_docs
    avgdl = r.avg_dl
    print(f"N={N}, avgdl={avgdl:.2f}")

    # get doc info for display
    doc_info = {}
    for row in sess.execute("SELECT doc_id, doc_title, doc_length FROM doc_stats"):
        doc_info[row.doc_id] = (row.doc_title, row.doc_length)

    # get postings for query terms
    postings = []
    for t in terms:
        rows = sess.execute(
            "SELECT term,doc_id,tf,df,doc_length FROM inverted_index WHERE term=%s", (t,)
        )
        for row in rows:
            postings.append((row.term, row.doc_id, row.tf, row.df, row.doc_length))

    sess.shutdown()
    cl.shutdown()

    if not postings:
        print("nothing found")
        spark.stop()
        sys.exit(0)

    print(f"found {len(postings)} postings")

    # use pyspark rdd to compute bm25
    rdd = sc.parallelize(postings)

    bc_n = sc.broadcast(N)
    bc_avg = sc.broadcast(avgdl)
    bc_k1 = sc.broadcast(K1)
    bc_b = sc.broadcast(B)

    def calc_bm25(entry):
        term, did, tf, df, dl = entry
        n = bc_n.value
        a = bc_avg.value
        k = bc_k1.value
        b = bc_b.value

        idf = math.log(n / df) if df > 0 else 0
        num = (k + 1) * tf
        den = k * ((1 - b) + b * (dl / a)) + tf
        return (did, idf * num / den)

    scores = rdd.map(calc_bm25).reduceByKey(lambda x, y: x + y)
    top = scores.takeOrdered(10, key=lambda x: -x[1])

    print(f"\ntop 10 results for: '{q}'")
    print(f"{'#':<4}{'DocID':<14}{'Score':<10}{'Title'}")
    for i, (did, score) in enumerate(top, 1):
        title = doc_info.get(did, ("?", 0))[0].replace("_", " ")
        print(f"{i:<4}{did:<14}{score:<10.4f}{title}")

    spark.stop()
