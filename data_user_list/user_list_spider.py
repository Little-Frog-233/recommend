# coding: utf-8
import sys

sys.path.append('../')

from spider_tool.moive_spider import douban_movie

if __name__ == '__main__':
	base_url = 'https://movie.douban.com/people/jennyqueen/collect?'
	douban = douban_movie(base_url,get_all=False)
	douban.main()
