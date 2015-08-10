from pyspark import SparkContext
import matplotlib.pyplot as plt

sc=SparkContext("local[2]","Movie Spark Example")
movie_data=sc.textFile("file://user/root/code/ml-100k/u.item")
print movie_data.first()

num_movies=movie_data.count()
print "Movie: %d " % num_movies

def convert_years(x):
    try:
        return int(x[-4])
    except:
        return 1900 # 'bad data point with a blank year set to 1900
movie_fields=movie_data.map(lambda lines: lines.split("|"))
years=movie_fields.map(lambda fields:fields[2]).map(lambda x: convert_year(x))

years_filtered = years.filter(lambda x:x != 1900)

movie_ages = years_filtered.map(lambda yr: 1998-yr).countByValue() 
values = movie_ages.values()
bins=movie_ages.keys()
plt.hist(value,bins=bins, color = 'lightblue', normed=True)
fig=plt.gcf()
fig.set_size_inches(16,10)
