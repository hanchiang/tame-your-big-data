# Calculate and display movie in ascending order of average rating
from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
	        MRStep(reducer=self.sorted_by_ratings)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, rating

    def reducer_count_ratings(self, movie, ratings):
        totalRating = 0
        ratingCount = 0
        for rating in ratings:
	        ratingCount += 1
	        totalRating += float(rating)
	    yield totalRating / ratingCount, movie

    def sorted_by_ratings(self, averageRating, movies):
	    for movie in movies:
	        yield movie, averageRating

if __name__ == '__main__':
    RatingsBreakdown.run()
