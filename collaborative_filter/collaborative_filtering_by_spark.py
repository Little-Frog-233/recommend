# coding: utf-8
import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row

spark = SparkSession.builder.appName('cf_demo').getOrCreate()

lines = spark.read.text(
	"/Users/ruicheng/PycharmProjects/Collaborative_filter/collaborative_filter/list.txt").rdd
parts = lines.map(lambda row: row.value.split("::"))
# ratingsRDD = parts.map(lambda p: Row(userId=str(p[0]), movieId=str(p[1]), rating=int(p[2]), timestamp=p[3]))
ratingsRDD = parts.map(lambda p: Row(userId=int(p[0]), movieId=int(p[1]), rating=int(p[2])))
ratings = spark.createDataFrame(ratingsRDD)
(training, test) = ratings.randomSplit([0.8, 0.2])

als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop")
model = als.fit(training)

predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")

rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))
#
# userRecs = model.recommendForAllUsers(3)
# userRecs.show()
#
# movieRecs = model.recommendForAllItems(3)
# movieRecs.show()

ratings.createOrReplaceTempView('ratings')
aimUser = spark.sql('select * from ratings where userId=1')
userR = model.recommendForUserSubset(aimUser,10)
userR.show()
userR.createOrReplaceTempView('userR')
res = spark.sql('select recommendations from userR')
result= res.rdd.collect()
for item in result:
	print(item)