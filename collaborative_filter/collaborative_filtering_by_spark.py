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
from sklearn.metrics.pairwise import cosine_similarity

spark = SparkSession.builder.appName('cf_demo').getOrCreate()
factory = douban_mysql()

lines = spark.read.text(
	"/Users/ruicheng/PycharmProjects/Collaborative_filter/collaborative_filter/list.txt").rdd
parts = lines.map(lambda row: row.value.split("::"))
ratingsRDD = parts.map(lambda p: Row(userId=int(p[0]), movieId=int(p[1]), rating=int(p[2])))
ratings = spark.createDataFrame(ratingsRDD)


# 根据不同的rank数值计算其mse,取使得mse最小的rank
def test_rank(list):
	'''
	输入测试的rank列表
	:param list:
	:return: 取最小为5
	'''
	for r in list:
		als = ALS(maxIter=5, rank=r, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
		          coldStartStrategy="drop")
		res = []
		for t in range(5):
			(training, test) = ratings.randomSplit([0.8, 0.2])
			model = als.fit(training)
			predictions = model.transform(test)
			evaluator = RegressionEvaluator(metricName="mse", labelCol="rating", predictionCol="prediction")
			mse = evaluator.evaluate(predictions)
			res.append(mse)
		mse = sum(res) / len(res)
		print("Rank = " + str(r))
		print("Mean-square error = " + str(mse))


def func_user(x):
	text = str(x)
	pattern_user = re.compile('userId=(\d+)')
	pattern_movie = re.compile('movieId=(\d+)')
	userId = re.findall(pattern_user, text)
	movieId = re.findall(pattern_movie, text)
	movieId = [int(i) for i in movieId]
	return (userId[0], movieId)


def func_movie(x):
	text = str(x)
	pattern_user = re.compile('userId=(\d+)')
	pattern_movie = re.compile('movieId=(\d+)')
	userId = re.findall(pattern_user, text)
	movieId = re.findall(pattern_movie, text)
	userId = [int(i) for i in userId]
	return (movieId[0], userId)


class cf_recommend:
	def __init__(self, rank):
		self.rank = rank
		self.als = ALS(maxIter=5, rank=self.rank, regParam=0.01, userCol="userId", itemCol="movieId",
		               ratingCol="rating",
		               coldStartStrategy="drop")
		self.model = self.als.fit(ratings)

	def movieRecommend(self, num=10):
		# 展示物品的特征向量
		items = self.model.itemFactors
		items_vector = items.rdd.map(lambda x: (x.id, x.features))
		item_vector_id = []
		item_vector_list = []
		for item in items_vector.collect():
			item_vector_id.append(item[0])
			item_vector_list.append(item[1])
		# 计算余弦相似度
		cos_sim = cosine_similarity(item_vector_list, item_vector_list)
		for i, choice in enumerate(item_vector_id):
			idx = np.argpartition(cos_sim[i, :], -(num + 1))[-(num + 1):].tolist()
			tmp = []
			for j in idx:
				tmp.append(item_vector_id[j])
			if choice in tmp:
				tmp.remove(choice)
			else:
				idx = np.argpartition(cos_sim[i, :], -num)[-num:].tolist()
				tmp = []
				for j in idx:
					tmp.append(item_vector_id[j])
			if factory.find_movie_in_rec(int(choice)):
				factory.update_recommend_movie(int(choice), str(tmp))
			else:
				factory.add_recommend_movie(int(choice), str(tmp))

	def userRecommend(self, num=20):
		userRecs = self.model.recommendForAllUsers(num)
		user_result = userRecs.rdd.map(func_user).collect()
		for item in user_result:
			if factory.find_user_in_rec(int(item[0])):
				factory.update_recommend_user(int(item[0]), str(item[1]))
			else:
				factory.add_recommend_user(int(item[0]), str(item[1]))

	def run(self, user=True, movie=True, userNum=20, movieNum=10):
		if user:
			self.userRecommend(userNum)
		if movie:
			self.movieRecommend(movieNum)


if __name__ == '__main__':
	test_rank([5,10,15,20,25])
	# cf_recommend = cf_recommend(5)
	# cf_recommend.run()
