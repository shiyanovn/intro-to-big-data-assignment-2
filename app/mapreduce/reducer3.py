import sys

# reducer3 - build inverted index
# input sorted: word\tdoc_id\ttitle\ttf\tdl\tdf
# output: word\tdf\tdoc_id:tf:dl,doc_id:tf:dl,...

cur_word = None
cur_df = 0
postings = []

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    cols = line.split("\t")
    if len(cols) < 6:
        continue
    w = cols[0]
    did = cols[1]
    tf = cols[3]
    dl = cols[4]
    df = cols[5]

    if cur_word and w != cur_word:
        # write out postings for previous word
        out = ",".join(f"{d}:{t}:{l}" for d, t, l in postings)
        print(f"{cur_word}\t{cur_df}\t{out}")
        postings = []

    cur_word = w
    cur_df = df
    postings.append((did, tf, dl))

if cur_word:
    out = ",".join(f"{d}:{t}:{l}" for d, t, l in postings)
    print(f"{cur_word}\t{cur_df}\t{out}")
