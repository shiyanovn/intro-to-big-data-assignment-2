import sys

# reducer2 - collect doc lengths, compute N and avgdl
# output - DOC\tdoc_id\ttitle\tdoc_length and STATS\tN\tavg_dl\ttotal_dl

all_docs = []

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    cols = line.split("\t")
    if len(cols) < 3:
        continue
    did = cols[0]
    title = cols[1]
    dl = int(cols[2])
    all_docs.append((did, title, dl))

n = len(all_docs)
total = sum(x[2] for x in all_docs)
avg = total / n if n > 0 else 0

for did, title, dl in all_docs:
    print(f"DOC\t{did}\t{title}\t{dl}")

print(f"STATS\t{n}\t{avg}\t{total}")
