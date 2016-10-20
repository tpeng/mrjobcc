"""
MRJob implementation of connected component algorithm from "Connected Components in MapReduce and Beyond"

<author: pengtaoo@gmail.com 2016-10-20>
"""

from mrjob.job import MRJob
from mrjob.protocol import ReprProtocol
from mrjob.step import MRStep


class ImportJob(MRJob):

    INTERNAL_PROTOCOL = ReprProtocol
    OUTPUT_PROTOCOL = ReprProtocol

    def mapper(self, _, line):
        v1, v2 = line.split(',')
        yield v1, v2


class CCJob(MRJob):

    INPUT_PROTOCOL = ReprProtocol
    INTERNAL_PROTOCOL = ReprProtocol
    OUTPUT_PROTOCOL = ReprProtocol

    def mapper_small_star(self, v1, v2):
        if v2 <= v1:
            yield v1, v2
        else:
            yield v2, v1

    def reducer_small_star(self, k, values):
        values = list(values)
        values.append(k)
        minv = min(values)

        for v in values:
            if k != minv:
                yield v, minv

    def mapper_large_star(self, v1, v2):
        yield v1, v2
        yield v2, v1

    def reducer_large_star(self, k, values):
        values = list(values)
        values.append(k)
        minv = min(values)

        for v in values:
            if v > k:
                yield v, minv

    def steps(self):
        return [
            MRStep(mapper=self.mapper_small_star, reducer=self.reducer_small_star),
            MRStep(mapper=self.mapper_large_star, reducer=self.reducer_large_star)
        ]


class CollectJob(MRJob):

    INPUT_PROTOCOL = ReprProtocol
    INTERNAL_PROTOCOL = ReprProtocol
    OUTPUT_PROTOCOL = ReprProtocol

    def mapper(self, k, v):
        yield k, v

if __name__ == "__main__":
    input_data = [
        [1,8],
        [8,9],
        [8,5],
        [8,7],
        [7,3],
        [3,2],
        [3,6],
        [6,4]
    ]
    with open('test.dot', 'w') as f:
        for v1, v2 in input_data:
            print >> f, '%s,%s' % (v1, v2)

    j1 = ImportJob(args=['test.dot', '--output', 'import-cc'])

    max_iterations = 10
    components = {}

    with j1.make_runner() as runner1:
        runner1.run()
        j1_input = runner1.get_output_dir()

        for i in range(max_iterations):
            j2 = CCJob([j1_input, '--output', 'star-cc'])
            with j2.make_runner() as runner2:
                runner2.run()
                j1_input = runner2.get_output_dir()

        j3 = CollectJob([j1_input, '--output', 'collect-cc'])
        with j3.make_runner() as runner3:
            runner3.run()
            for line in runner3.stream_output():
                k, v = j3.parse_output_line(line)
                components.setdefault(v, [v]).append(k)

    print components
