# coding:utf-8
import time
from . import log
from threading import Thread
from .downloader import DownloaderPool

class BaseSpiderPool():
    def __init__(self, num, task_generator,
                 urls_storage, items_storage,
                 name='Spider', log_path=''):
        self.runningFlag = False
        self.task_generator = task_generator()
        self.urls_storage = urls_storage
        self.items_storage = items_storage
        self.logger = log.generate_logger(name, log_path+'spider.log',
                                             log.INFO, log.INFO)
        self.spiders = [Thread(target=self._work) for i in range(num)]

    def _response_handle(self, str_content):
        urls = self.extract_urls_items(str_content)
        self.urls_storage(urls)

    def extract_urls_items(self, str_content):
        raise NotImplementedError(
            '''please implement this method to extract urls and items from str_content,
            use method self.urls_storage(items) for storage items, and return a set of urls''')

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
