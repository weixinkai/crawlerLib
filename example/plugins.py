#coding:utf8
import sys, os
lib_dir = os.path.abspath(os.path.dirname(os.getcwd())+os.path.sep+"..")
sys.path.append(lib_dir)

import sqlite3
import logging
import re
from pyquery import PyQuery as pq
from collections import deque
from threading import Thread
from crawlerLib.baseAnalyzer import BaseAnalyzer

class HtmlAnalyzer(BaseAnalyzer):
    def __init__(self, str_content):
        BaseAnalyzer.__init__(self)
        dom = pq(str_content)
        self._items = self.extract_items(dom)
        self._urls = self.extract_urls(dom)

    def extract_urls(self, dom):
        urls = set([tag.attr('href')
                    for tag in dom('a').filter('.page-numbers').items()])
        return urls

    def extract_items(self, dom):
        items = []
        for div in dom('.grid-8')('.post-meta').items():
            title = div(".archive-title:first").attr('title')
            url = div(".archive-title:first").attr('href')

            html = div('p:first').html()
            result = re.search(r'\d{4}/\d{2}/\d{2}', html)
            date = result.group().replace('/', '-') if result else '0000-00-00'
            items.append((title, date, url))
        return items


class SqliteConnector(Thread):
    def __init__(self, db_name):
        Thread.__init__(self)
        self.runningFlag = False
        self.items_queue = deque()
        self.db_name = db_name
        self.cursor = None
        self.logger = logging.getLogger()

    def stop(self):
        self.runningFlag = False

    def run(self):
        with sqlite3.connect(self.db_name) as conn:
            self.runningFlag = True
            self.cursor = conn.cursor()
            self._db_init(conn)
            while self.runningFlag or self.items_queue:
                try:
                    items = self.items_queue.popleft()
                    self._insert_items(items)                    
                except IndexError:
                    pass
                except Exception as e:
                    self.logger.error(e)
                finally:
                    conn.commit()
            conn.commit()
        self.logger.info('Items pipe closed!')

    def put_items(self, items):
        if type(items) != list and type(items) != set:
            raise TypeError('Items not a list or set')
        self.items_queue.append(set(items))

    def _db_init(self, conn):
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS collection
                                (title text, date text, url text)''')
        conn.commit()

    def _insert_items(self, items):
        ''' insert items  '''
        self.cursor.executemany('''INSERT INTO collection VALUES (?, ?, ?)''',
                                items)
