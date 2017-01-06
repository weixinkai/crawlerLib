# coding:utf-8
import time
import sys
import logging
from multiprocessing.dummy import Pool
from .downloader import DownloaderPool


class SpiderPool():
    def __init__(self, config, urls_storage, items_storage,
                 analyzer_class):
        worker_num = config['SpiderPool'].getint('thread_num', 4)
        self.pool = Pool(processes=worker_num)
        self.urls_storage = urls_storage
        self.items_storage = items_storage
        self.analyzer_class = analyzer_class
        self.logger = logging.getLogger('SpiderPool')

        self._items_count = 0

    def _response_handle(self, str_content):
        try:
            result = self.analyzer_class(str_content)
        except Exception as e:
            self.logger.error('Response analyze error! {0}'.format(e))

        self._extract_urls_handle(result.urls)
        self._extract_items_handle(result.items)

    def _extract_items_handle(self, items):
        if items is None:
            self.logger.debug('extract_items get None')
            return

        try:
            self._items_count += len(items)
        except Exception as e:
            self.logger.error('Fail to count items. {0}'.format(e))

        try:
            self.items_storage(items)
        except Exception as e:
            self.logger.error('Fail to storage items. {0}'.format(e))
            return

    def _extract_urls_handle(self, urls):
        if urls is None:
            self.logger.debug('extract_urls get None')
            return

        try:
            self.urls_storage(urls)
        except Exception as e:
            self.logger.error('Fail to storage urls. {0}'.format(e))
            return

    def stop(self):
        self.pool.close()
        self.pool.join()

    def add_task(self, text):
        self.pool.apply_async(self._response_handle, (text,))

    @property
    def items_count(self):
        return self._items_count
