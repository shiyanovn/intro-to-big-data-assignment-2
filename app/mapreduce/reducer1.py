import sys

# reducer1 - group by term, count df
# input - word\tdoc_id\ttitle\ttf\tdl
# output - word\tdoc_id\ttitle\ttf\tdl\tdf

cur_word = None
docs_list = []

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    cols = line.split("\t")
    if len(cols) < 5:
        continue
    w = cols[0]
    did = cols[1]
    title = cols[2]
    tf = cols[3]
    dl = cols[4]

    if cur_word and w != cur_word:
        # output all docs for previous word
        df = len(docs_list)
        for d_id, d_title, d_tf, d_dl in docs_list:
            print(f"{cur_word}\t{d_id}\t{d_title}\t{d_tf}\t{d_dl}\t{df}")
        docs_list = []

    cur_word = w
    docs_list.append((did, title, tf, dl))

# last word
if cur_word:
    df = len(docs_list)
    for d_id, d_title, d_tf, d_dl in docs_list:
        print(f"{cur_word}\t{d_id}\t{d_title}\t{d_tf}\t{d_dl}\t{df}")
