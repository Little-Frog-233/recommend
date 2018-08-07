# coding: utf-8
import findspark
import re
import numpy as np

findspark.init()

from douban_mysql import douban_mysql
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row

spark = SparkSession.builder.appName('cf_demo').getOrCreate()
factory = douban_mysql()

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


# ratings.createOrReplaceTempView('ratings')
# aimUser = spark.sql('select * from ratings where userId=1')
# userR = model.recommendForUserSubset(aimUser,10)
# userR.show()
# userR.createOrReplaceTempView('userR')
# res = spark.sql('select recommendations from userR')
# res.show()

aim_movie_id = 1
name = factory.find_movie_by_id(aim_movie_id)
print(r'现在查看的电影是%s' % name)
ratings.createOrReplaceTempView('ratings')
aimItem = spark.sql('select * from ratings where movieId=%s' % aim_movie_id)
userI = model.recommendForItemSubset(aimItem, 10)
userI.createOrReplaceTempView('userI')
res = spark.sql('select recommendations from userI')
result = res.rdd.collect()
pattern = re.compile('userId=(\d+)')
recommend = re.findall(pattern, str(result))
pattern_movie = re.compile('movieId=(\d+)')
recommend_movie_list = []
for item in recommend:
	tmp = spark.sql('select movieId from ratings where userId=%s limit 10' % item)
	text = str(tmp.rdd.collect())
	recommend_movie = re.findall(pattern_movie, text)
	recommend_movie_list.extend(recommend_movie)

if aim_movie_id in recommend_movie_list:
	recommend_movie_list.remove(aim_movie_id)
recommend_movie_r = np.random.choice(recommend_movie_list, 10, replace=False)
for movieId in recommend_movie_r:
	aim_name = factory.find_movie_by_id(int(movieId))
	print(r'看过%s的人也看过%s' % (name, aim_name))
