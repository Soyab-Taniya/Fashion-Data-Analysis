from mrjob.job import MRJob
from mrjob.step import MRStep

class PatternCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_pattern,
                   reducer=self.reducer_count_pattern)
        ]

    def mapper_get_pattern(self, _, line):
        (P_Id, Pattern) = line.split(',')
        yield Pattern, 1

    def reducer_count_pattern(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    PatternCount.run()

