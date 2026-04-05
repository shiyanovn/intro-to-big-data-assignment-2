import sys
import re
from cassandra.cluster import Cluster

# update cassandra when adding new doc

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


def main():
    if len(sys.argv) < 4:
        print("usage - add_doc_to_cassandra.py <doc_id> <doc_title> <file_path>")
        sys.exit(1)

    doc_id = sys.argv[1]
    doc_title = sys.argv[2]
    fpath = sys.argv[3]

    # read and tokenize
    with open(fpath, "r") as f:
        line = f.read()
    cols = line.split("\t", 2)
    if len(cols) >= 3:
        text = cols[2]
    else:
        text = line

    words = re.findall(r'[a-z0-9]+', text.lower())
    words = [w for w in words if w not in STOP and len(w) > 1]
    dl = len(words)

    # term frequencies
    freq = {}
    for w in words:
        freq[w] = freq.get(w, 0) + 1

    # connect cassandra
    cl = Cluster(['127.0.0.1'], port=9042)
    s = cl.connect('search_engine')

    # add doc_stats
    s.execute(
        "INSERT INTO doc_stats (doc_id, doc_title, doc_length) VALUES (%s,%s,%s)",
        (doc_id, doc_title, dl)
    )
    print(f"added doc_stats: {doc_id}, dl={dl}")

    # update corpus_stats
    row = s.execute("SELECT num_docs, avg_dl, total_dl FROM corpus_stats WHERE id=1").one()
    if row:
        new_n = row.num_docs + 1
        new_total = row.total_dl + dl
        new_avg = new_total / new_n
        s.execute(
            "UPDATE corpus_stats SET num_docs=%s, avg_dl=%s, total_dl=%s WHERE id=1",
            (new_n, new_avg, new_total)
        )
        print(f"corpus_stats updated: N={new_n}, avgdl={new_avg:.1f}")
    else:
        s.execute(
            "INSERT INTO corpus_stats (id,num_docs,avg_dl,total_dl) VALUES (1,%s,%s,%s)",
            (1, float(dl), dl)
        )

    # for each term, update inverted_index
    for w, tf in freq.items():
        existing = s.execute(
            "SELECT df FROM inverted_index WHERE term=%s LIMIT 1", (w,)
        ).one()
        if existing:
            new_df = existing.df + 1
        else:
            new_df = 1

        # insert new posting
        s.execute(
            "INSERT INTO inverted_index (term,doc_id,tf,df,doc_length) VALUES (%s,%s,%s,%s,%s)",
            (w, doc_id, tf, new_df, dl)
        )

        # update df for existing postings of this term
        if existing:
            rows = s.execute("SELECT doc_id FROM inverted_index WHERE term=%s", (w,))
            for r in rows:
                if r.doc_id != doc_id:
                    s.execute(
                        "UPDATE inverted_index SET df=%s WHERE term=%s AND doc_id=%s",
                        (new_df, w, r.doc_id)
                    )

    print(f"added {len(freq)} terms to inverted_index")

    s.shutdown()
    cl.shutdown()
    print("done")


if __name__ == "__main__":
    main()
