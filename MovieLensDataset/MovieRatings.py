from mrjob.job import MRJob
from mrjob.step import MRStep

class MovieRatings(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings)
        ]

    def mapper_get_ratings(self, _, line):
                # Splitting the input line into individual fields using '::' as the delimiter

        (userID, movieID, rating, timestamp) = line.split('::')
                # Emitting the rating as the key and a value of 1

        yield rating, 1

    def reducer_count_ratings(self, key, values):
         # Counting the occurrences of each rating by summing the values
        yield f"Rating {key}", sum(values)

if __name__ == '__main__':
    MovieRatings().run()


