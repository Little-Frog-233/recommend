# coding: utf-8
import os
import findspark

findspark.init()
from pyspark.sql import SparkSession
from spider_tool.moive_spider import douban_movie


def func(x):
	try:
		db = douban_movie(x[0], get_all=True)
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
	rdd.map(cut_line).repartition(10).foreach(func)
