# coding: utf-8
import sys

sys.path.append('../')

from spider_tool.moive_spider import douban_movie

proxyHost = "proxy.abuyun.com"
proxyPort = "9010"
proxyUser = "HTRS2F7560829W8P"
proxyPass = "67FCEB9CD8F667F8"
proxyMeta = "%(user)s:%(pass)s@%(host)s:%(port)s" % {
	"host": proxyHost,
	"port": proxyPort,
	"user": proxyUser,
	"pass": proxyPass,
}

if __name__ == '__main__':
	base_url = 'https://movie.douban.com/people/lykingking/collect?'
	douban = douban_movie(base_url, get_all=False, proxy=proxyMeta)
	douban.main()
