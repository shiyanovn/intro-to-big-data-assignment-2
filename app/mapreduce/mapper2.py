import sys
import re

# mapper2 - compute doc length
# input - doc_id\ttitle\ttext
# output - doc_id\ttitle\tdoc_length

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

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    cols = line.split("\t", 2)
    if len(cols) < 3:
        continue
    did = cols[0].strip()
    title = cols[1].strip()
    text = cols[2].strip()

    words = re.findall(r'[a-z0-9]+', text.lower())
    words = [w for w in words if w not in STOP and len(w) > 1]
    dl = len(words)
    print(f"{did}\t{title}\t{dl}")
