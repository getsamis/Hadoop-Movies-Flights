from mrjob.job import MRJob
from mrjob.step import MRStep

class PopularMovies(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_sorted_output)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('::')
        yield movieID, 1

    def reducer_count_ratings(self, key, values):
        yield None, (sum(values), key)

    def reducer_sorted_output(self, _, movie_counts):
        sorted_movies = sorted(movie_counts, reverse=True, key=lambda x: x[0])
        for count, movie in sorted_movies:
            yield movie, count

if __name__ == '__main__':
    PopularMovies.run()
