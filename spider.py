# coding:utf-8
import time
import sys
import logging
from threading import Thread
from .downloader import DownloaderPool


class SpiderPool():
    def __init__(self, config, task_generator, urls_storage, items_storage,
                 html_analyzer):
        self.runningFlag = False
        self.task_generator = task_generator()
        self.urls_storage = urls_storage
        self.items_storage = items_storage
        self.html_analyzer = html_analyzer
        self.logger = logging.getLogger('SpiderPool')

        num = config['SpiderPool'].getint('thread_num', 4)
        self.spiders = [Thread(target=self._work) for i in range(num)]
        self.items_count = 0

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

        self.items_count += len(items)
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

    def start(self):
        self.runningFlag = True
        for spider in self.spiders:
            spider.start()

    def stop(self):
        self.runningFlag = False

    def join(self):
        for spider in self.spiders:
            spider.join()

    def _work(self):
        while self.runningFlag:
            try:
                text = next(self.task_generator)
                if not text:
                    time.sleep(1)
                    continue
                self._response_handle(text)
            except StopIteration:
                break
            except Exception as e:
                self.logger.error(e)
        self.logger.debug('A spider stopped!')

    def get_items_count(self):
        return self.items_count
