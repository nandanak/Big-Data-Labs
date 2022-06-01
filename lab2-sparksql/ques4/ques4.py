
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlContext = SQLContext(sc)
# This path is to load text file and convert each line to Row
temprdd = sc.textFile("BDA/input/temperature-readings.csv")
lines = temprdd.map(lambda line: line.split(";"))

tempReadings = lines.map(lambda p: Row(station=p[0],  value = float(p[3])))

#Inferring the schema and registering the DataFrame as a table
schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.registerTempTable("tempReadings")

precipitationrdd = sc.textFile("BDA/input/precipitation-readings.csv")
lines = precipitationrdd.map(lambda line: line.split(";"))

precipitation = lines.map(lambda p: Row(station=p[0], date=p[1], year=p[1].split("-")[0], month = p[1].split("-")[1], day = p[1].split("-")[2], value = float(p[3])))

#Inferring the schema and registering the DataFrame as a table
schemaPrecip = sqlContext.createDataFrame(precipitation)
schemaPrecip.registerTempTable("precipitation")


#filter
schemaTempReadings = schemaTempReadings.groupBy('station').agg(F.max('value').alias('max_temperature'))
schemaTempReadings = schemaTempReadings.filter(schemaTempReadings['max_temperature'] >= 25)
schemaTempReadings = schemaTempReadings.filter(schemaTempReadings['max_temperature'] <= 30)

schemaPrecip = schemaPrecip.groupBy('station','year','month','day').agg(F.sum('value').alias('value'))
#filter
schemaPrecip = schemaPrecip.filter(schemaPrecip['value'] >= 100)
schemaPrecip = schemaPrecip.filter(schemaPrecip['value'] <= 200)

joinedAnswer = schemaTempReadings.join(schemaPrecip,['station']).select('station', 'max_temperature','value').orderBy(F.desc('station'))
#save output
joinedAnswer.rdd.saveAsTextFile("BDA/output")
