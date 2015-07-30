import re
import sys

call_cost_re = re.compile("call method (?P<cls>\w+)\.(?P<method>\w+) cost (?P<ns>\d+)")
for line in sys.stdin.readlines():
    match = call_cost_re.search(line)
    if match:
        cls = match.group('cls')
        method = match.group('method')
        ns = match.group('ns')
        print cls, method, ns
