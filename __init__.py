# coding:utf-8
import os
import sys
import logging
import logging.config
import configparser
from collections import deque
from redis import StrictRedis
from threading import Timer
from .downloader import DownloaderPool
from .spider import SpiderPool
from .redis_controller import RedisController
from .logging_conf import logging_config


def config_init():
    '''爬虫框架配置初始化'''
    config = configparser.ConfigParser()
    config_name = 'crawler.conf'

    if os.path.isfile(config_name):
        config.read(config_name)
    else:
        config['DownloaderPool'] = {'thread_num': 4,
                                    'freq': 1}
        config['SpiderPool'] = {'thread_num': 4}
        config['Redis'] = {'host' : '127.0.0.1',
                           'port' : 6379,
                           'db' : 0,
                           'task_url_name' : 'TaskURLs',
                           'seen_url_name' : 'SeenURLs'}
        with open(config_name, 'w') as configFile:
            config.write(configFile)
    return config


class CrawlerCoordinator:
    def __init__(self, analyzer_class, put_items_api):
        '''爬虫框架调度器'''
        
        #日志输出初始化
        self.logging_init()
        self.logger = logging.getLogger('Crawler')

        #配置初始化
        config = config_init()
        try:
            #检查redis是否连接得上
            self.urls_manager = RedisController(config)
            self.urls_manager.check_connect()
        except Exception as e:
            self.logger.error('Redis connect error! ' + str(e))
            sys.exit(0)

        #网页分析池
        self.spider_pool = SpiderPool(
            config=config,
            analyzer_class=analyzer_class,
            urls_storage=self.urls_manager.storage_urls,
            items_storage=put_items_api
        )

        #请求池
        self.downloader_pool = DownloaderPool(
            config=config,
            get_url=self.urls_manager.get_url,
            response_text_handle=self._response_text_handle
        )

        #响应计数
        self.response_count = 0

        #定时输出统计信息
        self.info_timer = None

    def start(self):
        '''启动爬虫框架'''
        self.logger.info('Crawler Start work...')
        
        #启动定时日志输出
        self._new_info_timer()

        #启动下载池
        self.downloader_pool.start()

    def stop(self):
        '''停止爬虫框架'''
        if self.info_timer: self.info_timer.cancel()
        self.logger.info('Crawler Stopping...')

        #停止各组件
        #先停止下载池保证不会再有新response
        self.downloader_pool.stop()
        self.logger.info('DownloaderPool stopped!')

        self.spider_pool.stop()
        self.logger.info('SpiderPool stopped!')
        
        self.logger.info('Crawler Stopped!')
        self.info()

    def logging_init(self):
        logging.config.dictConfig(logging_config)

    def task_init(self, start_urls, isFlushDB=False):
        '''insert start urls to db for crawler'''
        if not type(start_urls) is list:
            self.error(TypeError('task urls not list'))
            sys.exit(0)

        if isFlushDB:
            self.urls_manager.flushdb()
        self.urls_manager.storage_urls(start_urls)

    def _new_info_timer(self):
        '''定时输出日志'''
        if self.info_timer:
            self.info()

        self.info_timer = Timer(5, self._new_info_timer)
        self.info_timer.start()

    def _response_text_handle(self, response_text):
        '''put a response text from downloader'''
        self.response_count += 1
        self.spider_pool.add_task(response_text)

    def info(self):
        '''输出统计信息'''
        self.logger.info('========Crawler information========')
        self.logger.info('Seen urls: {0}, Task urls: {1}'
                         .format(self.urls_manager.seen_count, 
                                self.urls_manager.task_count))
        self.logger.info('Get responses: {0}'.format(self.response_count))
        self.logger.info('Extract items: {0}'.format(self.spider_pool.items_count))
        self.logger.info('-----------------------------------')
