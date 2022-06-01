from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
precipitate_file = sc.textFile("BDA/input/precipitation-readings.csv")
ostergotland_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
osterlines = ostergotland_file.map(lambda line: line.split(";"))
precpslines = precipitate_file.map(lambda line: line.split(";"))

#(station number, temperature)
station_oster = osterlines.map(lambda x: (x[0])).collect()

#((station number, date), precipitation)
monthly_precipitation = precpslines.map(lambda x: ((x[0], x[1][0:7]) ,(float(x[3]))))

#filter
monthly_precipitation = monthly_precipitation.filter(lambda x: x[0][0] in station_oster)
monthly_precipitation = monthly_precipitation.filter(lambda x: int (x[0][1][0:4]) >= 1993 and int (x[0][1][0:4]) <= 2016)


monthly_precipitation = monthly_precipitation.reduceByKey(lambda a,b: a + b)
monthly_precipitation = monthly_precipitation.map(lambda x: (x[0][1][0:7], (x[1], 1)))
count = monthly_precipitation.reduceByKey(lambda a,b: (a[0] + b[0], a[1]+b[1]))
avg_monthly_precipitation = count.map(lambda a: (a[0], a[1][0]/a[1][1]))



#print(max_temperatures.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
avg_monthly_precipitation.saveAsTextFile("BDA/output")
