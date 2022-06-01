from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value)
year_temperature = lines.map(lambda x: (x[1][0:7], float(x[3])))

#filter by year and temperature reading > 10
year_temperature = year_temperature.filter(lambda x: int(x[0][0:4])>=1950 and int(x[0][0:4])<=2014)
year_temperature = year_temperature.filter(lambda x: int(x[1])>10)

year_temperature = year_temperature.map(lambda x: (x[0], 1))

count = year_temperature.reduceByKey(lambda a,b: a+b)
count = count.sortBy(ascending = False, keyfunc=lambda k: k[1])

#print(count.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
count.saveAsTextFile("BDA/output")
