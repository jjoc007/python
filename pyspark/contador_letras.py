"""Cuanta cuantas lineas tienen la letra 'a' y cuantas lineas tienen la letra 'b'"""
from pyspark.sql import SparkSession

logFile = "test.txt"  # Should be some file on your system
spark = SparkSession.builder.appName("contador letras").getOrCreate()
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()