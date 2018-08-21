# coding: utf-8
import sys
import os
import configparser
import pymysql
import logging
import traceback

sys.path.append('../')


class douban_mysql:
	def __init__(self, recreate=False):
		'''
		第一次记得打开创建数据库的开关
		:param recreate: 是否创建数据库
		'''
		cf = configparser.ConfigParser()
		base_dir = os.path.dirname(__file__)
		cf.read(os.path.join(base_dir, "mysql.conf"))
		try:
			mysql_config = dict(cf.items("mysql"))
		except Exception as e:
			mysql_config = None
		# sqlalchemy 基本变量
		if mysql_config:
			self.db = pymysql.connect(host=mysql_config.get('mysql_host'), user=mysql_config.get('mysql_user'),
			                          password=mysql_config.get('mysql_pass'), port=3306,
			                          db=mysql_config.get('mysql_db'), charset='utf8')
		self.cursor = self.db.cursor()
		if recreate == True:
			sql_1 = 'CREATE TABLE IF NOT EXISTS douban_user (id INT AUTO_INCREMENT , name VARCHAR(255) NOT NULL, movie_all INT NOT NULL, home VARCHAR(255) NOT NULL, PRIMARY KEY (id))'
			self.cursor.execute(sql_1)
			sql_2 = 'CREATE TABLE IF NOT EXISTS douban_movie (id INT AUTO_INCREMENT , name VARCHAR(255) NOT NULL, summary TEXT NOT NULL, user_id INT NOT NULL,user_name VARCHAR(255) NOT NULL,score INT NOT NULL, movie_path VARCHAR(255) NOT NULL, PRIMARY KEY (id))'
			self.cursor.execute(sql_2)
			sql_3 = 'CREATE TABLE IF NOT EXISTS movie_list (id INT AUTO_INCREMENT ,name VARCHAR(255) NOT NULL, summary TEXT NOT NULL,movie_path VARCHAR(255) NOT NULL, PRIMARY KEY (id))'
			self.cursor.execute((sql_3))
			sql_4 = 'CREATE TABLE IF NOT EXISTS userRecs_list (id INT AUTO_INCREMENT ,user_id INT NOT NULL, movie_id TEXT NOT NULL, PRIMARY KEY (id))'
			self.cursor.execute(sql_4)
			sql_5 = 'CREATE TABLE IF NOT EXISTS movieRecs_list (id INT AUTO_INCREMENT ,movie_id INT NOT NULL, movie_id_list TEXT NOT NULL, PRIMARY KEY (id))'
			self.cursor.execute(sql_5)

	def add_user(self, username, movie_all, home):
		'''
		插入用户数据
		:param username: 用户名称
		:param movie_all: 用户评价电影总数
		:return: None
		'''
		sql = 'INSERT INTO douban_user(name, movie_all,home) values(%s,%s,%s)'
		try:
			self.cursor.execute(sql, (username, movie_all, home))
			self.db.commit()
		except:
			print('add douban_user fail')
			logging.error(traceback.print_exc())
			self.db.rollback()

	def update_user(self, id, movie_all):
		'''
		更新用户的列表
		:param id: 用户id
		:param movie_all:新的电影数量
		:return:
		'''
		sql = 'UPDATE douban_user SET movie_all = %s WHERE id=%s' % (movie_all, id)
		try:
			self.cursor.execute(sql)
			self.db.commit()
			print('update user_id:%s successful' % id)
		except:
			print('update douban_user fail')
			logging.error(traceback.print_exc())
			self.db.rollback()

	def add_movie(self, name, summary, user_id, username, score, movie_path):
		'''
		插入用户的电影数据
		:param name: 电影名称
		:param user_id: 用户id
		:param username: 用户名称
		:param score: 用户电影评分
		:return: None
		'''
		sql = 'INSERT INTO douban_movie(name,summary,user_id,user_name,score,movie_path) values(%s,%s,%s,%s,%s,%s)'
		try:
			self.cursor.execute(sql, (name, summary, user_id, username, score, movie_path))
			self.db.commit()
		except:
			print('add douban_movie fail')
			self.db.rollback()

	def find_user(self, username):
		'''
		查找用户是否已经存在，豆瓣用户名称唯一，因此可以以这个为去重判定条件
		:param username: 用户名称
		:return: 用户id
		'''
		sql = "SELECT * from douban_user WHERE name='%s'" % username
		self.cursor.execute(sql)
		one = self.cursor.fetchone()
		if one:
			print('user %s is already exists' % username)
			return int(one[0])
		else:
			return None

	def find_movie(self, name, movie_path, user_id):
		'''
		查找用户对应的电影是否存在
		:param name: 电影名称
		:param user_id: 用户id
		:return: 是否存在：True代表存在，False代表不存在
		'''
		sql = '''SELECT * from douban_movie WHERE movie_path="%s" and user_id=%s''' % (movie_path, user_id)
		self.cursor.execute(sql)
		one = self.cursor.fetchone()
		if one:
			print('''this moive %s scored by this user_id %s is already exists''' % (name, user_id))
			return True
		else:
			return False

	def find_500(self, username, MAX=500):
		'''
		查找是否有满足条件的500部电影
		:param user_id:
		:return:
		'''
		sql = "SELECT count(*) from douban_movie WHERE user_name='%s' and score>0" % username
		self.cursor.execute(sql)
		one = self.cursor.fetchone()
		if one and int(one[0]) > MAX:
			print('already get 500 movies')
			return True
		else:
			return False

	def find_all(self, username, MAX=500):
		'''
		查找是否有满足条件的给定数量的电影
		:param user_id:
		:return:
		'''
		sql = "SELECT count(*) from douban_movie WHERE user_name='%s'" % username
		self.cursor.execute(sql)
		one = self.cursor.fetchone()
		if one and int(one[0]) >= MAX:
			print('already get all movies')
			return True
		else:
			return False

	def get_movie_list(self, save_path, user_max=None, movie_max=500, get_movie=200):
		'''
		生成用于推荐系统的txt
		:param save_path: 保存位置
		:param user_max: 选取的最大用户数
		:param movie_max: 每个用户的最小电影数
		:param get_movie: 最终获取的每个用户的电影数
		:return:
		'''
		if user_max:
			sql = "select user_id from douban_movie GROUP BY user_id HAVING count(user_id)>%s limit %s;" % (
				movie_max, user_max)
		else:
			sql = "select user_id from douban_movie GROUP BY user_id HAVING count(user_id)>%s;" % movie_max
		self.cursor.execute(sql)
		all = self.cursor.fetchall()
		for item in all:
			id = item[0]
			sql_tmp = "SELECT user_id,name,score as number,movie_path FROM douban_movie WHERE score>0 and user_id=%s limit %s;" % (
				id, get_movie)
			self.cursor.execute(sql_tmp)
			results = self.cursor.fetchall()
			for result in results:
				try:
					movie_id = self.find_movie_id(result[3], result[1])
					movie_id != None
					with open(save_path, 'a') as f:
						f.write(str(result[0]) + '::' + str(movie_id) + '::' + str(result[2]) + '\n')
				except:
					print('movie %s is not in movie_list' % result[0])


def add_movie_list_all(self):
	'''
	将douban_movie中的所有电影插入到movie_list中并生成每个电影对应的id
	:return:
	'''
	sql = 'SELECT name,summary,DISTINCT movie_path FROM douban_movie'
	self.cursor.execute(sql)
	all = self.cursor.fetchall()
	for item in all:
		id = self.find_movie_id(item[2], item[0])
		if id == None:
			sql_movie = 'INSERT INTO movie_list(name,summary,movie_path) values(%s,%s,%s)'
			try:
				self.cursor.execute(sql_movie, (item[0], item[1], item[2]))
				self.db.commit()
			except:
				print('add douban_movie fail')
				self.db.rollback()
		else:
			print('''this movie %s is already exists''' % item[0])


def add_movie_list(self, name, summary, movie_path):
	'''
	将一部电影插入movie_list
	:param name: 电影名称
	:param summary: 电影简介
	:param movie_path: 电影主页面
	:return:
	'''
	sql_movie = 'INSERT INTO movie_list(name,summary,movie_path) values(%s,%s,%s)'
	try:
		self.cursor.execute(sql_movie, (name, summary, movie_path))
		self.db.commit()
	except:
		print('add douban_movie fail')
		self.db.rollback()


def add_recommend_user(self, userId, movieId):
	'''
	基于用户的推荐列表
	:param userId:
	:param movieId:
	:return:
	'''
	sql = 'INSERT INTO userRecs_list(user_id,movie_id) values(%s,%s)'
	try:
		self.cursor.execute(sql, (userId, movieId))
		self.db.commit()
	except:
		print('add douban_movie fail')
		self.db.rollback()


def update_recommend_user(self, userId, movieId):
	'''
	更新基于用户的推荐列表
	:param userId:
	:param movieId:
	:return:
	'''
	sql = '''UPDATE userRecs_list SET movie_id='%s' WHERE user_id=%s''' % (movieId, userId)
	try:
		self.cursor.execute(sql)
		self.db.commit()
		print('update userRecs_list successful')
	except:
		print('update userRecs_list fail')
		self.db.rollback()


def add_recommend_movie(self, movieId, userId):
	'''
	基于电影的推荐列表
	:param movieId:
	:param userId:
	:return:
	'''
	sql = 'INSERT INTO movieRecs_list(movie_id,movie_id_list) values(%s,%s)'
	try:
		self.cursor.execute(sql, (movieId, userId))
		self.db.commit()
	except:
		print('add douban_movie fail')
		self.db.rollback()


def update_recommend_movie(self, movieId, userId):
	'''
	更新基于用户的推荐列表
	:param userId:
	:param movieId:
	:return:
	'''
	sql = '''UPDATE movieRecs_list SET movie_id_list='%s' WHERE movie_id=%s''' % (userId, movieId)
	try:
		self.cursor.execute(sql)
		self.db.commit()
		print('update movieRecs_list successful')
	except:
		print('update movieRecs_list fail')
		self.db.rollback()


def find_movie_id(self, movie_path, name):
	'''
	查询一部电影是否已经在movie_list中
	:param movie_path: 电影主页面
	:param name: 电影名称
	:return:
	'''
	sql = "SELECT id FROM movie_list WHERE movie_path='%s'" % movie_path
	self.cursor.execute(sql)
	one = self.cursor.fetchone()
	if one:
		print('''this movie %s is already exists in movie_list''' % name)
		return int(one[0])
	else:
		return None


def find_movie_by_id(self, id):
	'''
	根据id查找相应的电影
	:param id:
	:return:
	'''
	sql = "SELECT name FROM movie_list WHERE id=%s" % id
	self.cursor.execute(sql)
	one = self.cursor.fetchone()
	if one:
		return one[0]
	else:
		return None


def find_user_by_id(self, id):
	'''
	根据id查找对应的用户
	:param id:
	:return:
	'''
	sql = "SELECT name FROM douban_user WHERE id=%s" % id
	self.cursor.execute(sql)
	one = self.cursor.fetchone()
	if one:
		return one[0]
	else:
		return None


def find_user_in_rec(self, id):
	'''
	在推荐列表中查找用户用于更新推荐
	:param id:
	:return:
	'''
	sql = "SELECT * FROM userRecs_list where user_id=%s" % id
	self.cursor.execute(sql)
	one = self.cursor.fetchone()
	if one:
		print('this userId %s is already exists' % id)
		return True
	else:
		return False


def find_movie_in_rec(self, id):
	'''
	在推荐列表中查找电影用于更新推荐
	:param id:
	:return:
	'''
	sql = "SELECT * FROM movieRecs_list where movie_id=%s" % id
	self.cursor.execute(sql)
	one = self.cursor.fetchone()
	if one:
		print('this movieId %s is already exists' % id)
		return True
	else:
		return False


def userRec_list(self, id):
	'''
	查找指定用户的推荐列表
	:param id:用户id
	:return: 推荐列表
	'''
	sql = "SELECT movie_id FROM userRecs_list WHERE user_id=%s" % id
	self.cursor.execute(sql)
	one = self.cursor.fetchone()
	if one:
		return [int(i) for i in one[0].strip('[').strip(']').split(',')]
	else:
		print('this user %s not in userRecs_list' % id)
		return None


def movieRec_list(self, id):
	'''
	查找指定电影的推荐列表
	:param id: 电影id
	:return: 推荐列表
	'''
	sql = "SELECT movie_id_list FROM movieRecs_list WHERE movie_id=%s" % id
	self.cursor.execute(sql)
	one = self.cursor.fetchone()
	if one:
		return [int(i) for i in one[0].strip('[').strip(']').split(', ')]
	else:
		print('this movie %s not in movieRecs_list' % id)
		return None


if __name__ == '__main__':
	# factory = douban_mysql(True)
	factory = douban_mysql()
	print(factory.movieRec_list(1))
	print(factory.userRec_list(1))
