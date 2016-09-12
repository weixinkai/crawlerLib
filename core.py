# coding:utf-8
import os, sys
from collections import deque
from redis import StrictRedis
from threading import Timer
from .downloader import DownloaderPool
from .spider import BaseSpiderPool
from . import log

class CrawlerCoordinator():
    def __init__(self, items_pipe,
                 spider_num=4, downloader_num=10,
                 spiderPool_class=BaseSpiderPool,
                 downloaderPool_class=DownloaderPool,
                 log_path='log/'):
        self.runningFlag = False
        self.name = 'Crawler Coordinator'
        log_path = log_path.rstrip('/\\') + '/'
        self.logger = self._init_logger(log_path)

        self.url_db = StrictRedis()
        try:
            self.url_db.ping()
        except Exception as e:
            self.logger.error('Redis error! ' + str(e))
            sys.exit(0)

        self.task_url_name = 'TaskURLs'
        self.seen_url_name = 'SeenURLs'

        self.response_text_queue = deque()

        if not hasattr(items_pipe, 'put_items'):
            self.logger.error(NotImplementedError('Please implement method put_items(items) in items_pip for sending items to pipe'))
            sys.exit(0)

        self.items_pipe = items_pipe

        self.spider_pool = spiderPool_class(num=spider_num,
                                            task_generator=self._response_text_generator,
                                            urls_storage=self._urls_storage,
                                            items_storage=self._put_items,
                                            log_path=log_path)
        self.downloader_pool = downloaderPool_class(num=downloader_num,
                                                    task_generator=self._url_generator,
                                                    response_text_handle=self._put_response_text,
                                                    log_path=log_path)

        self.items_count = 0
        self.response_count = 0

        self.info_timer = None

    def start(self):
        self.logger.info('CrawlerCoordinator Starting work...')
        self.runningFlag = True
        self._new_info_timer()
        self.spider_pool.start()
        self.downloader_pool.start()

    def stop(self):
        if self.info_timer: self.info_timer.cancel()
        self.logger.info('CrawlerCoordinator Stopping...')

        self.downloader_pool.stop()
        self.downloader_pool.join()
        self.logger.info('DownloaderPool stopped!')

        self.logger.info('Wait for response handle...')
        while self.response_text_queue: pass
        self.logger.info('Response handle finish!')

        self.spider_pool.stop()
        self.spider_pool.join()
        self.logger.info('SpiderPool stopped!')

        self.info()
        self.logger.info('CrawlerCoordinator Stopped!')

    def _init_logger(self, log_path):
        if not os.path.exists(log_path):
            os.mkdir(log_path)
        return log.generate_logger(self.name, log_path+'Core.log',
                                   log.INFO, log.INFO)

    def _new_info_timer(self):
        if self.info_timer:
            self.info()

        if self.runningFlag:
            self.info_timer = Timer(5, self._new_info_timer)
            self.info_timer.start()

    def _put_response_text(self, response_text):
        '''put a response text from downloader'''
        self.response_text_queue.append(response_text)
        self.response_count += 1

    def _response_text_generator(self):
        '''get a response text for spider'''
        while self.runningFlag or self.response_text_queue:
            text = None
            try:
                text = self.response_text_queue.popleft()
            except IndexError:
                self.logger.debug('No response!')
            except Exception as e:
                self.logger.error(e)
            finally:
                yield text

    def _put_items(self, items):
        '''pass the items to sqlite connector from spider'''
        try:
            self.items_pipe.put_items(items)
            self.items_count += len(items)
        except Exception as e:
            self.logger.error(e)

    def _url_generator(self):
        '''get url from url db for downloader'''
        while self.runningFlag:
            url = self.url_db.lpop(self.task_url_name)
            yield url.decode('utf-8') if url else None

    def _urls_storage(self, urls):
        '''storage urls extracted by spider'''
        if not urls:return

        pipe = self.url_db.pipeline()
        for url in urls:
            pipe.sadd(self.seen_url_name, url)
        flags = pipe.execute()
        if not 1 in flags: return

        new_urls = filter(lambda x:x, (url if flag else False
                                       for flag, url in zip(flags, urls)))
        pipe.rpush(self.task_url_name, *new_urls)
        pipe.execute()

    def info(self):
        seen_urls_count = self.url_db.scard(self.seen_url_name)
        task_urls_count = self.url_db.llen(self.task_url_name)

        self.logger.info('Seen urls: {0}, Task urls: {1}'.format(seen_urls_count, task_urls_count))
        self.logger.info('Get response: {0}, Response wait for handle: {1}, Extracte items: {2}'
                         .format(self.response_count, len(self.response_text_queue), self.items_count))
