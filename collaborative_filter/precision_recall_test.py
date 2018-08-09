# coding: utf-8
import findspark
findspark.init()
import re
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row

spark = SparkSession.builder.appName('cf_demo').getOrCreate()
lines = spark.read.text(
    "/Users/ruicheng/PycharmProjects/Collaborative_filter/collaborative_filter/list.txt").rdd
parts = lines.map(lambda row: row.value.split("::"))
# ratingsRDD = parts.map(lambda p: Row(userId=str(p[0]), movieId=str(p[1]), rating=int(p[2]), timestamp=p[3]))
ratingsRDD = parts.map(lambda p: Row(userId=int(p[0]), movieId=int(p[1]), rating=int(p[2])))
ratings = spark.createDataFrame(ratingsRDD)
als = ALS(maxIter=5, rank=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
              coldStartStrategy="drop")
(training, test) = ratings.randomSplit([0.8, 0.2])
model = als.fit(training)


def func_user(x):
	text = str(x)
	pattern_user = re.compile('userId=(\d+)')
	pattern_movie = re.compile('movieId=(\d+)')
	userId = re.findall(pattern_user, text)
	movieId = re.findall(pattern_movie, text)
	movieId = [int(i) for i in movieId]
	return (int(userId[0]), movieId)


num_list = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
precision_list = []
recall_list = []
for num in num_list:
	items_test = test.rdd.map(lambda x: (x.userId, x.movieId)).groupByKey().map(lambda x: (x[0], list(x[1]))).collect()
	userPd = model.recommendForUserSubset(test, num)
	item_predict = userPd.rdd.map(func_user).collect()
	p_list = []
	Ru = []
	Tu = []
	for i in item_predict:
		for j in items_test:
			if i[0] == j[0]:
				p_list.append(len(set(i[1]) & set(j[1])))
				Ru.append(len(i[1]))
				Tu.append(len(j[1]))
	print('num is %s' % num)
	print('precision is:', sum(p_list) / sum(Ru) * 100, '%')
	precision_list.append(sum(p_list) / sum(Ru) * 100)
	print('recall is:', sum(p_list) / sum(Tu) * 100, '%')
	recall_list.append(sum(p_list) / sum(Tu) * 100)

