from pyspark import SparkConf, SparkContext
import sys
import time

conf = SparkConf().setAppName("DistribuiraniSistemi").setMaster(
    "spark://192.168.137.1:7077")
sc = SparkContext(conf=conf)
sc.setLogLevel("OFF")

path = "kupovina.csv" 

input_rdd = sc.textFile(path)

purchases = input_rdd.map(lambda line: line.split(",")) \
    .map(lambda parts: (parts[0], round(float(parts[2]), 2)))

total_spent_by_customer = purchases.reduceByKey(lambda x, y: x + y)

for customer, total_spent in total_spent_by_customer.collect():
    print(f"{customer} {total_spent}")

sys.exit(0)
