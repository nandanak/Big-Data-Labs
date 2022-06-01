
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlContext = SQLContext(sc)
# This path is to load text file and convert each line to Row
rdd = sc.textFile("BDA/input/temperature-readings.csv")
lines = rdd.map(lambda line: line.split(";"))

tempReadings = lines.map(lambda p: Row(station=p[0], date=p[1], year=p[1].split("-")[0], month = p[1].split("-")[1],time = p[2], value = float(p[3]), quality=p[4]))

#Inferring the schema and registering the DataFrame as a table
schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.registerTempTable("tempReadings")

#filter
schemaTempReadings = schemaTempReadings.filter(schemaTempReadings['year'] >= 1950)
schemaTempReadings = schemaTempReadings.filter(schemaTempReadings['year'] <= 2014)
schemaTempReadings = schemaTempReadings.filter(schemaTempReadings['value'] >= 10)
#distict temperature readings
countDistinct_temperatures = schemaTempReadings.select('station','year','month').distinct().groupBy('year','month').count().orderBy(F.desc('count'))


# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
countDistinct_temperatures.rdd.saveAsTextFile("BDA/output")
