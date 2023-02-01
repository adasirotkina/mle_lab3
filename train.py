import cx_Oracle as orcCon
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.context import SparkContext, SparkConf
from pyspark import SparkContext
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SQLContext
from pyspark.ml.evaluation import ClusteringEvaluator
from tqdm.auto import tqdm



user = os.environ.get("PYTHON_USER", "system")

dsn = os.environ.get("PYTHON_CONNECT_STRING", "localhost/XE")

pw = os.environ.get("PYTHON_PASSWORD")
if pw is None:
    pw = 'ada8970884'

conn = orcCon.connect(user, pw, dsn)
cursor = conn.cursor()
# Execute query
sql = "SELECT * FROM product"
cursor.execute(sql)

FEATURES_COL =  ['energy_kcal_100g',
 'energy_100g',
 'fat_100g',
 'saturated_fat_100g',
 'carbohydrates_100g',
 'sugars_100g',
 'proteins_100g',
 'salt_100g',
 'sodium_100g']

# Fetch all the records
result = pd.DataFrame(cursor.fetchall())
result.columns = ['code'] + FEATURES_COL

conf = SparkConf().set("spark.cores.max", "16") \
    .set("spark.driver.memory", "16g") \
    .set("spark.executor.memory", "16g") \
    .set("spark.executor.memory_overhead", "16g") \
    .set("spark.driver.maxResultsSize", "0")

sc = SparkContext('local')

sqlContext = SQLContext(sc)
result.to_csv('input.csv', index=False)
path = 'input.csv'

# df = sqlContext.read.csv(path, header=True) # requires spark 2.0

lines = sc.textFile(path)
data = lines.map(lambda line: line.split(","))
data.take(2)

df = data.toDF(['code'] + FEATURES_COL)

df = df.na.drop()
# df.show()

for col in df.columns:
    if col in FEATURES_COL:
        df = df.withColumn(col,df[col].cast('float'))
df = df.na.drop()

vecAssembler = VectorAssembler(inputCols=FEATURES_COL, outputCol="features")
df_kmeans = vecAssembler.transform(df).select('code', 'features')

kmeans = KMeans().setK(4).setSeed(1).setFeaturesCol("features")
model = kmeans.fit(df_kmeans)

# predictions = model.transform(df_kmeans)

transformed = model.transform(df_kmeans).select('code', 'prediction')
rows = transformed.collect()
print(rows[:3])

df_pred = sqlContext.createDataFrame(rows).toPandas()

cursor.execute("BEGIN EXECUTE IMMEDIATE 'DROP TABLE result'; EXCEPTION WHEN OTHERS THEN NULL; END;")
cursor.execute("CREATE TABLE result (code varchar2(40) NOT NULL, prediction number)")

for i, row in df_pred.iterrows():
    # sql = "INSERT INTO iris(sepal_length,sepal_width,petal_length,petal_width,species) VALUES(:1,:2,:3,:4,:5)"
    sql = "INSERT INTO result(code, prediction) VALUES(:1,:2) "
    cursor.execute(sql, tuple(row))
# the connection is not autocommitted by default, so we must commit to save our changes
conn.commit()
print("Record inserted succesfully")

cursor.close()
conn.close()