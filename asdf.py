from datetime import date, datetime

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

print("==============")
print(spark)

df = spark.createDataFrame([
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
], schema='a long, b double, c string, d date, e timestamp')

print("==============")
df.show()
df.printSchema()
df.coalesce(1).write.csv("/share/sample.csv")
# df.toPandas().to_csv("/share/panda.csv", index=False)
# df2 = spark.read.csv("/share/panda.csv", header=True, inferSchema=True)
df2 = spark.read.csv("/share/sample.csv")
print("=== df2 ===")
df2.show()
df2.printSchema()