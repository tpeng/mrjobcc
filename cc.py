"""
MRJob implementation of connected component algorithm from "Connected Components in MapReduce and Beyond"

<author: pengtaoo@gmail.com 2016-10-20>
"""

from mrjob.job import MRJob
from mrjob.protocol import ReprProtocol
from mrjob.step import MRStep


class CCJob(MRJob):

    INTERNAL_PROTOCOL = ReprProtocol
    OUTPUT_PROTOCOL = ReprProtocol

    def mapper_import(self, _, line):
        v1, v2 = line.split(',')
        yield v1, v2

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

    def mapper_export(self, k, v):
        yield k, v

    def steps(self):
        _steps = [MRStep(mapper=self.mapper_import)]
        for i in range(10):
            _steps.append(MRStep(mapper=self.mapper_small_star, reducer=self.reducer_small_star))
            _steps.append(MRStep(mapper=self.mapper_large_star, reducer=self.reducer_large_star))
        _steps.append(MRStep(mapper=self.mapper_export))
        return _steps

if __name__ == '__main__':
    CCJob().run()