import sys
import MapReduce

mr = MapReduce.MapReduce(sys.argv[0])


def mapper(relation):
    person = relation[0]
    friend = relation[1]
    mr.emit_intermediate(person, 1)


def reducer(person, list_of_values):
    total = 0
    for v in list_of_values:
        total += v
    mr.emit((person, total))


if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
