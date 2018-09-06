# coding: utf-8
import sys
sys.path.append('/Users/ruicheng/PycharmProjects/Collaborative_filter/')
import os
import findspark

findspark.init()
from pyspark.sql import SparkSession
from spider_tool.moive_spider import douban_movie


def get_proxy(proxyUser, proxyPass):
	'''
	阿布云代理设置
	:param proxyUser: http隧道通行证书
	:param proxyPass: http隧道通行密钥
	:return:
	'''
	proxyHost = "proxy.abuyun.com"
	proxyPort = "9020"
	proxyUser = proxyUser
	proxyPass = proxyPass
	proxyMeta = "%(user)s:%(pass)s@%(host)s:%(port)s" % {
		"host": proxyHost,
		"port": proxyPort,
		"user": proxyUser,
		"pass": proxyPass,
	}
	return proxyMeta


def func(x):
	try:
		# db = douban_movie(x[0], get_all=False, proxy=get_proxy("HO71Q57K620ZKRED", "E30286E11CB9A974"))
		db = douban_movie(x[0], get_all=False)
		db.main()
	except:
		print('error happened and the name is ', x[1])


def cut_line(x):
	line = x.split()
	return line


if __name__ == '__main__':
	current_path = os.path.realpath(__file__)
	# 获取当前文件的父目录
	father_path = os.path.realpath(os.path.dirname(current_path) + os.path.sep + ".")
	path = os.path.join(father_path, 'douban_user_list_2.txt')
	spark = SparkSession.builder.appName('Spark-Spider').getOrCreate()
	sc = spark.sparkContext
	rdd = sc.textFile(path)
	rdd.map(cut_line).repartition(5).foreach(func)

