# coding: utf-8
import re
import logging
import traceback
pattern = re.compile('rating(\d+)-t')###因为pyquery出了问题所以用正则表达式来提取

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from pyquery import PyQuery as pq

url = 'https://movie.douban.com/people/7542909/collect?start=0&sort=time&rating=all&filter=all&mode=grid'
path = '/Users/ruicheng/chromedriver01/chromedriver'
browser = webdriver.Chrome(path)
wait = WebDriverWait(browser, 5)

browser.get(url)
try:
	wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.article .item')))
	html = browser.page_source
	html = html.replace('<!DOCTYPE html>', '')
	doc = pq(html)
	print('user is:', doc('.side-info-txt').text().replace('\n>\n豆瓣主页', ''))
	items = doc('.article .item').items()
	for item in items:
		print('title is:', item('.pic .nbg').attr('title'))
		text = item.html()  ###pyquery不能直接选择节点名称了，垃圾
		gread = re.findall(pattern, text)
		if len(gread) == 0:
			print('gread is:', 0)
		else:
			print('gread is:', gread[0])
except:
	print('some error happened')
	logging.error(traceback.print_exc())
browser.close()
