import sys

# mapper3 - just pass through hadoop will sort by first column
for line in sys.stdin:
    line = line.strip()
    if line:
        print(line)
