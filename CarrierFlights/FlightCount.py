from mrjob.job import MRJob
from mrjob.step import MRStep

class FlightCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_carrier,
                   reducer=self.reducer_count_flights)
        ]

    def mapper_get_carrier(self, _, line):
        fields = line.split(',')
        carrier = fields[8]  # Extract the carrier code from the 9th field
        yield carrier, 1

    def reducer_count_flights(self, carrier, counts):
        yield carrier, sum(counts)

if __name__ == '__main__':
    FlightCount.run()
