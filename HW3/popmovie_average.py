from mrjob.job import MRJob
from mrjob.step import MRStep


class PopMovie(MRJob):
    def steps(self):
        return [
                MRStep(mapper=self.map_rating_average,
                        combiner=self.combine_rating_average,
                        reducer=self.reduce_rating_average),
                MRStep(reducer=self.reduce_sort)
                ]
    def map_rating_average(self,_,line):
        userid,movieid,rating,timestamp=line.split(',')
        if userid!='userId':
            yield (movieid,(float(rating),1))
    def combine_rating_average(self,movie_id,values):
        total_rating=0
        count=0
        for rating, cnt in values:
            total_rating+=rating
            count+=cnt
        yield (movie_id,(total_rating,count))
    def reduce_rating_average(self,movie_id,values):
        total_rating=0
        count=0
        for rating, cnt in values:
            total_rating+=rating
            count+=cnt
        yield (str(total_rating/count).ljust(6,'0')[:6], movie_id)
    def reduce_sort(self,avg_rating,movie_ids):
        for movie in movie_ids:
            yield (movie, avg_rating)

if __name__=='__main__':
    PopMovie.run()
