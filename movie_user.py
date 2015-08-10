from pyspark import SparkContext
import matplotlib.pyplot as plt

sc=SparkContext("local[2]","Movie Spark Example")
user_data=sc.textFile("file://user/root/code/ml-100k/u.user")
user_data.first()
user_fields=user_data.map(lambda line: line.split("|"))
num_users=user_fields.map(lambda fields: fields[0]).count()
num_genders=user_fields.map(lambda fields: fields[2]).distinct().count()
num_occupations=user_fields.map(lambda fields: fields[3]).distinct().count()
num_zipcodes=user_fields.map(lambda fields: fields[4]).distinct().count()
print "Users: %d, genders: %d,occupations: %d, ZIP Codes: %d" % (num_users,num_genders,num_occupations,num_zipcodes)

ages=user_fields.map(lambda x: int(x[1])).collect()
plt.hist(ages,bins=20,color='lightblue', normed=True)
fig=plt.gcf()
fig.set_size_inches(16,10)

count_by_occupation=user_fields.map(lambda fields: (fields[3],1)).reduceByKey(lambda x,y:x+y).collect()

x_axis1=np.array([c[0] for c in count_by_occupation])
y_axis1=np.array([c[1] for c in count_by_occupation])

x_axis=x_axis1[np.argsort(y_axis1)]
y_axis=y_axis1[np.argsort(y_axis1)]

pos=np.arange(len(x_axis))

width=1.0

ax=plt.axes()
ax.set_xticks(pos + (width/2))
ax.set_xticklabels(x_axis)

plt.bar(pos,y_axis, width, color='lightblue')
plt.xticks(rotation=30)
fig=plt.gcf()
fig.set_size_inches(16,10)

count_by_occupation2 = user_fields.map(lambda fields: fields[3]).countByValue()
print "Map-reduce approach"
print dict(count_by_occupation2)
print ""
print "countByValue approach"
print dict(count_by_occupation)





