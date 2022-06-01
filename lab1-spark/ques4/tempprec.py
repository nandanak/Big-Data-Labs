from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
tempslines = temperature_file.map(lambda line: line.split(";"))
precpslines = precipitation_file.map(lambda line: line.split(";"))

#(station number, temperature)
station_temperature = tempslines.map(lambda x: (x[0] ,float(x[3])))
#((station number, date), precipitation)
daily_precipitation = precpslines.map(lambda x: ((x[0], x[1]) ,float(x[3])))
#filter

station_temperature = station_temperature.reduceByKey(lambda x,y: x if x>=y else y)
station_temperature = station_temperature.filter(lambda x: int(x[1])>25 and int(x[1])<30)
daily_precipitation = daily_precipitation.reduceByKey(lambda a,b: a + b)
daily_precipitation = daily_precipitation.map(lambda x: (x[0][0], x[1]))
daily_precipitation = daily_precipitation.reduceByKey(lambda x,y: x if x>=y else y)
daily_precipitation = daily_precipitation.filter(lambda x: int(x[1])>100 and int(x[1])<200)

tempsprecps = station_temperature.join(daily_precipitation)
tempsprecps = tempsprecps.map(lambda x: (x[0], x[1], x[3]))

#print(max_temperatures.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
tempsprecps.saveAsTextFile("BDA/output")
