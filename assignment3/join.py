import MapReduce
import sys

mr = MapReduce.MapReduce(sys.argv[0])


def mapper(records):
    table_id = records[0]
    order_id = records[1]
    mr.emit_intermediate(order_id, records)


def reducer(order_id, attrs):
    out = []
    for attr in attrs:
        out.append(attr)
    mr.emit(out)


if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
