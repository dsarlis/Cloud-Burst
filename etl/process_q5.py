import sys

list = []
for line in sys.stdin:
    parts = line.split("\t")
    list.append((int(parts[0]), int(parts[1])))

sorted_list = sorted(list, key=lambda tup: tup[0])

cumulative = 0
cumulative_off_by_one = 0
for user, count in sorted_list:
    cumulative_off_by_one = cumulative
    cumulative += count
    print "%d\t%d\t%d\t%d" % (user, count, cumulative, cumulative_off_by_one)
