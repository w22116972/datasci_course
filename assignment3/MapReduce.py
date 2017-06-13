import json


class MapReduce:
    def __init__(self, file_name):
        self.intermediate = {}
        self.result = []
        self.file_name = file_name[:-3]

    def emit_intermediate(self, key, value):
        self.intermediate.setdefault(key, [])
        self.intermediate[key].append(value)

    def emit(self, value):
        self.result.append(value)

    def execute(self, data, mapper, reducer):
        for line in data:
            record = json.loads(line)
            mapper(record)

        for key in self.intermediate:
            reducer(key, self.intermediate[key])

        jenc = json.JSONEncoder()
        with open(self.file_name + '.json', 'w') as outfile:
            for item in self.result:
                # print(jenc.encode(item))
                outfile.write(jenc.encode(item) + '\n')
                # outfile.write(json.dumps(jenc.encode(item)))
                # outfile.writelines(json.dumps(jenc.encode(item)))
        import subprocess
        subprocess.run(["diff", "-y", "--suppress-common-lines", self.file_name + '.json', "./solutions/" + self.file_name + ".json"])