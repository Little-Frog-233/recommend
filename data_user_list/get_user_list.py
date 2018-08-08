# coding: utf-8
import requests
import re
import logging
import traceback
import os
import time
from pyquery import PyQuery as pq
from douban_mysql import douban_mysql

headers = {
	'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
	'Cookie': 'll="118162"; bid=U56wYJDbrhk; __yadk_uid=JHaRtzO0RveGY2aklii3FhXUynaRNSKP; _vwo_uuid_v2=DD862A0E8F58D429AA1C18B903F91E656|d17d153455f722d9ae2235cce0e09c21; __utmc=30149280; __utmc=223695111; ct=y; douban-fav-remind=1; ps=y; ue="1342468180@qq.com"; push_noty_num=0; push_doumail_num=0; __utmv=30149280.17960; __utmz=223695111.1532661025.12.4.utmcsr=douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/people/itzhaoxiangyu/; ap=1; __utma=30149280.546083573.1527832081.1532673361.1532676725.15; __utmz=30149280.1532676725.15.5.utmcsr=accounts.douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/safety/unlock_sms/resetpassword; dbcl2="179601793:74q7qMOcPrY"; ck=hBW1; _pk_ref.100001.4cf6=%5B%22%22%2C%22%22%2C1532676742%2C%22https%3A%2F%2Fwww.douban.com%2Fpeople%2Fitzhaoxiangyu%2F%22%5D; _pk_ses.100001.4cf6=*; __utma=223695111.344951148.1527832081.1532673361.1532676742.15; __utmb=223695111.0.10.1532676742; __utmt=1; __utmb=30149280.13.10.1532676725; _pk_id.100001.4cf6=0710816efa653b27.1527832081.15.1532679218.1532674708.'
}

factory = douban_mysql()


def get_user_list(url):
	response = requests.get(url, headers=headers)
	html = response.text
	doc = pq(html)
	items = doc('.comment-item').items()
	for item in items:
		res = {}
		res['user_url'] = item('.comment-info a').attr('href')
		res['user_name'] = item('.comment-info a').text().replace(' ', '-')
		temp = get_user_main(res['user_url'])
		if temp:
			res['user_collect'] = temp
		else:
			continue
		yield res


def get_user_main(url):
	response = requests.get(url, headers=headers)
	html = response.text
	doc = pq(html)
	movie = doc('#movie')
	if not movie:
		return None
	items = movie('.pl a').items()
	for item in items:
		if 'collect' in item.attr('href') and get_number(item):
			return item.attr('href') + '?'


def get_number(item, numMax=900):
	s = item.text()
	pattern = re.compile('(\d+).*')
	n = re.findall(pattern, s)
	num = int(n[0])
	if num > numMax:
		return True
	return False


if __name__ == '__main__':
	url_base = 'https://movie.douban.com/subject/1292402/comments?start=%d&limit=20&sort=new_score&status=P'
	current_path = os.path.realpath(__file__)
	# 获取当前文件的父目录
	father_path = os.path.realpath(os.path.dirname(current_path) + os.path.sep + ".")
	file_path = os.path.join(father_path, 'douban_user_list_2.txt')
	count = 0
	for i in range(0, 10):
		try:
			print('start to spider page:', i)
			index = 20 * i
			url = url_base % index
			for res in get_user_list(url):
				id = factory.find_user(res['user_name'])
				if id:
					continue
				count += 1
				with open(file_path, 'a') as f:
					f.write(res['user_collect'] + ' ' + res['user_name'] + '\n')
				print(res['user_collect'], res['user_name'])
			time.sleep(2)
		except:
			print('error in page:', i)
			logging.error(traceback.print_exc())
	print(count)
