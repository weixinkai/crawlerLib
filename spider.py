# coding:utf-8
import time
import sys
import logging
from multiprocessing.dummy import Pool
from threading import Thread
from .downloader import DownloaderPool


class SpiderPool():
    def __init__(self, config, urls_storage, items_storage,
                 html_analyzer):
        worker_num = config['SpiderPool'].getint('thread_num', 4)
        self.pool = Pool(processes=worker_num)
        self.urls_storage = urls_storage
        self.items_storage = items_storage
        self.html_analyzer = html_analyzer
        self.logger = logging.getLogger('SpiderPool')

        self._items_count = 0

    def _response_handle(self, str_content):
        try:
            urls, items = self.html_analyzer.extract_urls_items(str_content)
            self._extract_urls_handle(urls)
            self._extract_items_handle(items)
        except Exception as e:
            self.logger.error('Response handle error! {0}'.format(e))

    def _extract_items_handle(self, items):
        if items is None:
            self.logger.debug('extract_items get None')
            return

        if not hasattr(items, '__iter__'):
            self.logger.error('Extract items not iterable')
            return

        self._items_count += len(items)
        try:
            self.items_storage(items)
        except Exception as e:
            self.logger.error('Fail to put items to items pipe. {0}'.format(e))

    def _extract_urls_handle(self, urls):
        if urls is None:
            self.logger.debug('extract_urls get None')
            return
        try:
            if not type(urls) is list:
                urls = list(urls)
        except Exception as e:
            self.logger.error('Fail to convert extracted urls to list. {0}'.format(e))
            return
        self.urls_storage(urls)

    def stop(self):
        self.pool.close()
        self.pool.join()

    def add_task(self, text):
        self.pool.apply_async(self._response_handle, (text,))

    @property
    def items_count(self):
        return self._items_count
