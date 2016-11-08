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
    def __init__(self, html_analyzer, items_pipe):
        '''爬虫框架调度器'''
        self.runningFlag = False

        #日志输出初始化
        self.logging_init()
        self.logger = logging.getLogger('Crawler')

        #配置初始化
        config = config_init()

        #检查接口是否齐全
        self._check_api(html_analyzer, items_pipe)
        try:
            #检查redis是否连接得上
            self.urls_manager = RedisController(config)
            self.urls_manager.check_connect()
        except Exception as e:
            self.logger.error('Redis connect error! ' + str(e))
            sys.exit(0)

        #响应队列
        self.response_text_queue = deque()

        #网页分析池
        self.spider_pool = SpiderPool(
            config=config,
            html_analyzer=html_analyzer,
            task_generator=self._response_text_generator,
            urls_storage=self.urls_manager.storage_urls,
            items_storage=items_pipe.put_items
        )

        #请求池
        self.downloader_pool = DownloaderPool(
            config=config,
            task_generator=self.urls_manager.url_generator,
            response_text_handle=self._put_response_text
        )

        #响应计数
        self.response_count = 0

        #定时输出统计信息
        self.info_timer = None

    def _check_api(self, html_analyzer, items_pipe):
        '''检查插件接口是否齐全'''
        flag = False
        if not hasattr(html_analyzer, 'extract_urls_items'):
            self.logger.error(NotImplemented('Not found func extract_urls_items return [urls, items] in analyzer'))
            flag = True

        if not hasattr(items_pipe, 'put_items'):
            self.logger.error(NotImplementedError('Please implement method put_items(items) in items_pip for sending items to pipe'))
            flag=True
        if flag:
            sys.exit(0)

    def start(self):
        '''启动爬虫框架'''
        self.logger.info('Crawler Starting work...')
        self.runningFlag = True

        #启动定时日志输出
        self._new_info_timer()

        #启动各组件
        self.urls_manager.start_url_generator()
        self.spider_pool.start()
        self.downloader_pool.start()

    def stop(self):
        '''停止爬虫框架'''
        if self.info_timer: self.info_timer.cancel()
        self.logger.info('Crawler Stopping...')

        #停止各组件
        self.urls_manager.stop_url_generator()
        self.downloader_pool.stop()
        self.downloader_pool.join()
        self.logger.info('DownloaderPool stopped!')

        #先停止下载池保证不会再有新response
        self.runningFlag = False
        self.spider_pool.stop()
        self.spider_pool.join()
        self.logger.info('SpiderPool stopped!')

        self.info()
        self.logger.info('Crawler Stopped!')

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

    def info(self):
        '''输出统计信息'''
        self.logger.info('========Crawler information========')
        self.logger.info('Seen urls: {0}, Task urls: {1}'
                         .format(self.urls_manager.seen_count, 
                                self.urls_manager.task_count))
        self.logger.info('Get responses: {0}, Task responses: {1}'
                         .format(self.response_count, len(self.response_text_queue)))
        self.logger.info('Extract items: {0}'.format(self.spider_pool.items_count))
        self.logger.info('-----------------------------------')
