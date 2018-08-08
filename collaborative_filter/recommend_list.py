# coding: utf-8
import numpy as np
from douban_mysql import douban_mysql


class douban_recommend:
	def __init__(self):
		'''
		初始化
		'''
		self.factory = douban_mysql()

	def get_userRec(self, userId, num=10):
		'''
		根据用户id推荐电影
		:param userId:
		:return:
		'''
		list = self.factory.userRec_list(userId)
		if num > len(list):
			print('wrong happened')
			return
		list_r = np.random.choice(list,num)
		for item in list_r:
			name = self.factory.find_movie_by_id(item)
			print('''猜你喜欢:%s''' % name)

	def get_movieRex(self, movieId, num=10):
		'''
		根据电影id推荐电影
		:param movieId:
		:return:
		'''
		list = self.factory.movieRec_list(movieId)
		aimname = self.factory.find_movie_by_id(movieId)
		for item in list[:num + 1]:
			name = self.factory.find_movie_by_id(item)
			print('''看过 %s 的用户也喜欢 %s''' % (aimname, name))


if __name__ == '__main__':
	doubanRec = douban_recommend()
	doubanRec.get_userRec(1)
	doubanRec.get_movieRex(1)
