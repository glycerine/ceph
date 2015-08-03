from os.path import basename
import glob

def avg(d):
    return sum(d) / len(d)

for path in glob.glob("results/*.dat"):
    filename = basename(path)
    input_file = open(path, 'r')
    latencies = []
    for line in input_file.readlines():
        parts = line.split(": ")
        ns = long(parts[1])
        ms = float(ns) / 1000000
        latencies.append(ms)
    print filename, min(latencies), max(latencies), avg(latencies)
