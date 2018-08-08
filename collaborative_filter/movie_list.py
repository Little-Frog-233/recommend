# coding: utf-8
from douban_mysql import douban_mysql

if __name__ == '__main__':
	factory = douban_mysql()
	# factory.add_movie_list_all()
	factory.get_movie_list('/Users/ruicheng/PycharmProjects/Collaborative_filter/collaborative_filter/list.txt',user_max=300)