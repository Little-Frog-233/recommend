# coding: utf-8
import sys
sys.path.append('../')
import os
import findspark

findspark.init()
from pyspark.sql import SparkSession


def func(x):
	try:
		db = douban_movie(x[0], get_all=True)
		db.main()
	except:
		print('error happened and the name is ', x[1])


def cut_line(x):
	line = x.split()
	return line


################################################################
import re
import sys
import time
import logging
import traceback

sys.path.append('../')
from urllib.parse import urlencode

import requests
from pyquery import PyQuery as pq


class douban_movie(object):
	def __init__(self, url_base, get_all=True, proxy=None, cookie=None):
		'''
		初始化
		:param url_base: 初始网页
		:param get_all: 控制是否全爬
		'''
		self.url_base = url_base
		self.pattern = re.compile('rating(\d+)-t')  ###因为pyquery出了问题所以用正则表达式来提取
		if cookie:
			self.headers = {
				'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
				'Cookie': cookie
			}
		else:
			self.headers = {
				'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'
			}
		self.proxy = proxy
		if self.proxy:
			self.proxies = {'http': 'http://' + self.proxy, 'https': 'https://' + self.proxy}
		self.factory = douban_mysql()  ###数据库初始化
		self.get_all = get_all

	def get_all_messages(self):
		'''
		获得该用户的最大页码和名称和电影总数
		:return: int:pageMax str:username int(movie_number)
		'''
		pattern_number = re.compile('\((\d+)\)')
		res = []
		url = self.url_base
		home = url
		try:
			if self.proxy:
				response = requests.get(url, headers=self.headers, proxies=self.proxies)
			else:
				response = requests.get(url, headers=self.headers)
			response.status_code == 200
		except:
			logging.error(traceback.print_exc())
			print('your ip is baned')
			return None
		html = response.text
		doc = pq(html)
		items = doc('.paginator a').items()
		user_name = doc('.side-info-txt h3').text()
		all_number = doc('.info h1').text()
		movie_number = re.findall(pattern_number, all_number)[0]
		for item in items:
			res.append(item.text())
		id = self.factory.find_user(username=user_name)
		if not id:
			self.factory.add_user(user_name, int(movie_number), home)
		return int(res[-2]), user_name, int(movie_number)

	def get_message(self, index, username):
		'''
		获得该用户当前页面的所有电影信息
		:param index: 页码
		:return: 用户，电影名称，评分(1-5)
		'''
		try:
			params = {
				'start': index,
				'sort': 'time',
				'ratind': 'all',
				'filter': 'all',
				'mode': 'gird'
			}
			url = self.url_base + urlencode(params)
			if self.proxy:
				response = requests.get(url, headers=self.headers, proxies=self.proxies)
			else:
				response = requests.get(url, headers=self.headers)
			response.status_code == 200
			html = response.text
			doc = pq(html)
			user = username
			id = self.factory.find_user(user)
			items = doc('.article .item').items()
			for item in items:
				result = {}
				result['user'] = user
				print('title is:', item('.info em').text())
				result['title'] = item('.info em').text()
				text = item.html()  ###pyquery不能直接选择节点名称了，垃圾
				gread = re.findall(self.pattern, text)
				result['movie_home'] = item('.info a').attr('href')
				result['summary'] = item('.intro').text()
				if len(gread) == 0:
					result['gread'] = 0
					print('grade is', result['gread'])
				else:
					result['gread'] = int(gread[0])
					print('gread is', result['gread'])
				try:
					if not self.factory.find_movie(result['title'], result['movie_home'], id):
						self.factory.add_movie(result['title'], result['summary'], id, user, result['gread'],
						                       result['movie_home'])
					if not self.factory.find_movie_id(result['movie_home'], result['title']):
						self.factory.add_movie_list(result['title'], result['summary'], result['movie_home'])
				except:
					logging.error(traceback.print_exc())
					continue
		except:
			print('some error happened')
			logging.error(traceback.print_exc())

	def main(self):
		try:
			pageMax, username, movie_number = self.get_all_messages()
			if not self.get_all:
				id = self.factory.find_user(username)
				if id:
					if self.factory.find_500(username):
						print('the user %s is already exists and his movie is over 500' % username)
						return
			try:
				for i in range(0, pageMax + 1):
					print('start to spider page:', i)
					time.sleep(2)
					index = i * 15
					self.get_message(index, username)
					time.sleep(2)
					if not self.get_all:
						if self.factory.find_500(username):  ###增加暂停语句
							break
			except:
				logging.error(traceback.print_exc())
		except:
			logging.error(traceback.print_exc())


#############################################################################

if __name__ == '__main__':
	current_path = os.path.realpath(__file__)
	# 获取当前文件的父目录
	father_path = os.path.realpath(os.path.dirname(current_path) + os.path.sep + ".")
	path = os.path.join(father_path, 'douban_user_list_2.txt')
	spark = SparkSession.builder.appName('Spark-Spider').getOrCreate()
	sc = spark.sparkContext

	from douban_mysql import douban_mysql

	rdd = sc.textFile(path)
	rdd.map(cut_line).repartition(10).foreach(func)
