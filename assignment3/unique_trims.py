import MapReduce
import sys

mr = MapReduce.MapReduce(sys.argv[0])


def mapper(kv):
    seq_id = kv[0]
    nucleotide = kv[1]
    mr.emit_intermediate(nucleotide[:-10], 0)


def reducer(nucleotide, list_of_values):
    total = 0
    for v in list_of_values:
        total += 0
    mr.emit(nucleotide)


if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
