# coding: utf-8
import sys
sys.path.append('../')
import os

if __name__ == '__main__':
	current_path = os.path.realpath(__file__)
	# 获取当前文件的父目录
	father_path = os.path.realpath(os.path.dirname(current_path) + os.path.sep + ".")
	print(father_path)
	# url_base = 'https://movie.douban.com/people/1233038/collect?start=%d&sort=time&rating=all&filter=all&mode=grid'
	# douban_movie.main(url_base)
