import sys
import os
import re
import pyarrow.parquet as pq
from pyspark.sql import SparkSession

# read parquet, make txt files, upload to hdfs, build /input/data with pyspark rdd

def clean_name(s):
    # remove bad chars, replace space with _
    s = re.sub(r'[^\w\s\-.]', '', s)
    s = s.replace(' ', '_')
    s = re.sub(r'_+', '_', s)
    return s.strip('_')


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "/data/a.parquet"
    num = int(sys.argv[2]) if len(sys.argv) > 2 else 100

    print(f"reading {path}, need {num} docs")

    # read parquet with pyarrow
    pf = pq.ParquetFile(path)
    docs = []
    for batch in pf.iter_batches(batch_size=500, columns=['id', 'title', 'text']):
        d = batch.to_pydict()
        for i in range(len(d['id'])):
            txt = d['text'][i]
            if txt and len(txt.strip()) > 0:
                docs.append({
                    'id': d['id'][i],
                    'title': d['title'][i] or "untitled",
                    'text': txt
                })
            if len(docs) >= num:
                break
        if len(docs) >= num:
            break

    print(f"got {len(docs)} docs")

    # save as txt files locally then upload
    tmp = "/tmp/doc_files"
    os.makedirs(tmp, exist_ok=True)

    for doc in docs:
        did = str(doc['id'])
        t = doc['title']
        fname = did + "_" + clean_name(t) + ".txt"
        fpath = os.path.join(tmp, fname)
        with open(fpath, "w", encoding="utf-8") as f:
            f.write(doc['text'])

    print(f"wrote {len(os.listdir(tmp))} files to {tmp}")

    # put to hdfs
    os.system("hdfs dfs -rm -r -f /data/*")
    os.system(f"hdfs dfs -put {tmp}/*.txt /data/")
    print("uploaded to HDFS /data")

    # build /input/data using pyspark rdd
    spark = SparkSession.builder \
        .appName('prepare') \
        .master("local[2]") \
        .config("spark.driver.memory", "512m") \
        .getOrCreate()
    sc = spark.sparkContext

    files_rdd = sc.wholeTextFiles("hdfs://master:9000/data/*.txt")

    def make_line(item):
        fpath, content = item
        name = fpath.split("/")[-1]
        name = name.rsplit(".", 1)[0]
        parts = name.split("_", 1)
        did = parts[0]
        title = parts[1] if len(parts) > 1 else "untitled"
        # clean up text - remove tabs/newlines
        txt = content.replace("\t", " ").replace("\n", " ").replace("\r", " ")
        txt = re.sub(r'\s+', ' ', txt).strip()
        return f"{did}\t{title}\t{txt}"

    result = files_rdd.map(make_line)
    os.system("hdfs dfs -rm -r -f /input/data")
    result.coalesce(1).saveAsTextFile("hdfs://master:9000/input/data")

    print("saved /input/data")
    spark.stop()
    print("done")


if __name__ == "__main__":
    main()
