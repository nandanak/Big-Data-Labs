from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value)
year_temperature = lines.map(lambda x: ((x[1][0:7],x[0]) ,float(x[3])))

#filter
year_temperature = year_temperature.filter(lambda x: int(x[0][0][0:4])>=1960 and int(x[0][0][0:4])<=2014)

sumcount = year_temperature.combineByKey(lambda avg: (avg, 1), lambda x,avg: (x[0] + avg, x[1] + 1), lambda x,y: (x[0] + y[0], x[1] + y[1]))
avg_temperature = sumcount.map(lambda  (k, (sum, count)): (k, sum/count))


#print(max_temperatures.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
avg_temperature.saveAsTextFile("BDA/output")
