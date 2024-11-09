from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class PopMovie(MRJob):
    JOBCONF = {
        'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
        'mapreduce.partition.keycomparator.options': '-k1,1r'
    }
    def steps(self):
        return [
                MRStep(mapper=self.map_rating_average,
                        reducer=self.reduce_join),
                MRStep(reducer=self.reduce_rating_average),
                MRStep(reducer=self.reduce_sort)
                ]
    def map_rating_average(self,_,line):
        reader=csv.reader([line])
        for row in reader:
            if len(row)==4:
                userid,movieid,rating,timestamp=row
                if userid!='userId':
                    year=str(1970+(int(timestamp)//31536000))
                    yield (movieid,('rating',float(rating),1,year))
            else:
                movieid,title,genre=row
                if movieid!='movieId':
                    yield (movieid, ('movie',title))
    def reduce_join(self,movie_id,values):
        rating_data=[]
        title=None
        for value in values:
            if value[0]=='rating':
                rating_data.append(value[1:])
            else:
                title=value[1]
        if title:
            for data in rating_data:
                rating,count,year=data
                yield ((movie_id,year), (rating,count,title))
    def reduce_rating_average(self,movie_data,values):
        total_rating=0
        count=0
        for rating, cnt, title in values:
            total_rating+=rating
            count+=cnt
        yield (str(total_rating/count).ljust(6,'0')[:6], (title,movie_data[1]))
    def reduce_sort(self,avg_rating,title_data):
        for title,year in title_data:
            msg=f'{title}\'s {year} average rating:'
            yield (msg, avg_rating)

if __name__=='__main__':
    PopMovie.run()
