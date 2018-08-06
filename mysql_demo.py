# -*- coding: utf-8 -*-
import functools
import sys
import os
import configparser

sys.path.append('../')
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engines = {}
cf = configparser.ConfigParser()
base_dir = os.path.dirname(__file__)
cf.read(os.path.join(base_dir, "mysql.conf"))
try:
    mysql_config = dict(cf.items("mysql"))
except Exception as e:
    mysql_config = None

# sqlalchemy 基本变量
if mysql_config:
    global_engine = create_engine('mysql://%s:%s@%s:%d/%s?charset=utf8' % (
        mysql_config.get('mysql_user'), mysql_config.get('mysql_pass'), mysql_config.get('mysql_host'), 3306,
        mysql_config.get('mysql_db')), encoding='utf8', echo=False, pool_recycle=3600)
else:
    global_engine = create_engine(
        'mysql://root:@127.0.0.1/high_reads_logs?charset=utf8', echo=False,
        pool_recycle=3600)

engines['main'] = global_engine
sessions = {}
for key, engine in engines.items():
    sessions[key] = sessionmaker(bind=engine)


def main_db_with_main_thread(method):
    """
    """

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        self.db_session = sessions.get('main')() if sessions.get('main') else None
        try:
            return method(self, *args, **kwargs)

        finally:
            self.db_session.commit()
            self.db_session.expunge_all()
            self.db_session.close()

    return wrapper