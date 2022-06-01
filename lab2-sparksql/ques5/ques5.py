
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlContext = SQLContext(sc)
# This path is to load text file and convert each line to Row
stationrdd = sc.textFile("BDA/input/stations-Ostergotland.csv")
lines = stationrdd.map(lambda line: line.split(";"))

stations = lines.map(lambda p: Row(station=p[0]))

#Inferring the schema and registering the DataFrame as a table
schemaStations = sqlContext.createDataFrame(stations)
schemaStations.registerTempTable("stations")
len = schemaStations.count()

precipitationrdd = sc.textFile("BDA/input/precipitation-readings.csv")
lines = precipitationrdd.map(lambda line: line.split(";"))

precipitation = lines.map(lambda p: Row(station=p[0], year=p[1].split("-")[0], month = p[1].split("-")[1], day = p[1].split("-")[2], value = float(p[3])))

#Inferring the schema and registering the DataFrame as a table
schemaPrecip = sqlContext.createDataFrame(precipitation)
schemaPrecip.registerTempTable("precipitation")

#filter
precipOster = schemaPrecip.filter(schemaPrecip['year'] >= 1993)
precipOster = precipOster.filter(precipOster['value'] <= 2016)

precipOster = schemaPrecip.join(schemaStations, ['station']).select('year','month','value')
precipOster = precipOster.groupBy('year','month').agg((F.sum('value')/ len).alias('value')).orderBy(F.desc('value'))
#save output
precipOster.rdd.repartition(1).saveAsTextFile("BDA/output")
