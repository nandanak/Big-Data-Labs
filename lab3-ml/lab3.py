from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext

sc = SparkContext(appName="lab_kernel")

def haversine(lon1, lat1, lon2, lat2):
	"""
	Calculate the great circle distance between two points on the
	earth (specified in decimal degrees)
	"""
	# convert decimal degrees to radians
	lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
	# haversine formula
	dlon = lon2 - lon1
	dlat = lat2 - lat1
	a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
	c = 2 * asin(sqrt(a))
	km = 6367 * c
	return km

h_distance = 100 # 100 km
h_date = 5 # 5 days
h_time = 3*3600 # 10800 seconds or 3 hours
a = 58.4274
b = 14.826
date = "2013-07-04"

stations_file = sc.textFile("BDA/input/stations.csv")
temps_file = sc.textFile("BDA/input/temperature-readings.csv")
stations = stations_file.map(lambda line: line.split(";"))
stations = stations.map(lambda x: (int(x[0]), float(x[3]), float(x[4])))
temps = temps_file.map(lambda line: line.split(";"))
temps = temps.map(lambda x: (int(x[0]),x[1],x[2],float(x[3])))

#Gaussian kernel for Distance
def kernelfordistance(long1,lat1,long2,lat2,h):
	distance = haversine(long1,lat1,long2,lat2)
	rdist = exp(-((distance/h)**2))
	return rdist
#Gaussian kernel for Date
def kernelfordate(x,date,h):
	diffdate = (datetime(int(x[0:4]),int(x[5:7]),int(x[8:10])) - datetime(int(date[0:4]),int(date[5:7]),int(date[8:10]))).days
	rdate = exp(-((diffdate/h)**2))
	return rdate
#Gaussian kernel for Time
def kernelfortime(x,time,date,h):
	difftime = (datetime(int(date[0:4]),int(date[5:7]),int(date[8:10]),int(x[0:2]),int(x[3:5]),int(x[6:8])) - datetime(int(date[0:4]),int(date[5:7]),int(date[8:10]),int(time[0:2]),int(time[3:5]),int(time[6:8]))).seconds
	rtime = exp(-((difftime/h)**2))
	return rtime

distkernel = stations.map(lambda x: (x[0], kernelfordistance(x[2],x[1],b,a,h_distance)))
diststations = sc.broadcast(distkernel.collectAsMap())
#Filtering on the dates
datefilter = temps.filter(lambda x: (datetime(int(date[0:4]),int(date[5:7]),int(date[8:10])) >= datetime(int(x[1][0:4]),int(x[1][5:7]),int(x[1][8:10]))))
#caching the filtered data
datefilter.cache()
#creating dictionary for sum and product of kernels
temperature_sum = {}
temperature_product = {}

for time in ["00:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00", "12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]:
	#Filtering on the times
	datefilter = datefilter.filter(lambda x: (datetime(int(date[0:4]),int(date[5:7]),int(date[8:10])) == datetime(int(x[1][0:4]),int(x[1][5:7]),int(x[1][8:10]))))
	timefilter = datefilter.filter(lambda x: (datetime(int(date[0:4]),int(date[5:7]),int(date[8:10]),int(time[0:2]),int(time[3:5]),int(time[6:8])) >= datetime(int(x[1][0:4]),int(x[1][5:7]),int(x[1][8:10]),int(x[2][0:2]),int(x[2][3:5]),int(x[2][6:8]))))
	kernel = timefilter.map(lambda x: (diststations.value[x[0]], kernelfordate(x[1],date,h_date), kernelfortime(x[2],time,date,h_time),x[3]))

	#Sum of kernels
	sum = kernel.map(lambda x: (x[0] + x[1] + x[2],x[3]))
	sum = sum.map(lambda x: (x[0]*x[1],x[0]))
	sum = sum.reduce(lambda x,y: (x[0]+y[0],x[1]+y[1]))
	temperature_sum[time] = sum[0]/sum[1]

	#Product of kernels
	product = kernel.map(lambda x: (x[0] * x[1] * x[2],x[3]))
	product = product.map(lambda x: (x[0]*x[1],x[0]))
	product = product.reduce(lambda x,y: (x[0]+y[0],x[1]+y[1]))
	temperature_product[time] = product[0]/product[1]

print("**** KERNEL SUM!! ****")
print(temperature_sum)
print("**** KERNEL PRODUCT!! ****")
print(temperature_product)
