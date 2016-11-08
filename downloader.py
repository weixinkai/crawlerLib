# coding:utf-8
import time
import asyncio
import aiohttp
import logging
from threading import Thread

class DownloaderPool(Thread):
    def __init__(self, config, task_generator, response_text_handle):
        '''
            请求线程池
            task_generator : URL生成器
            response_text_handle : 响应处理接口
        '''
        Thread.__init__(self)
        self.runningFlag = False
        self.worker_num = config['DownloaderPool'].getint('thread_num', 4)
        self.freq = config['DownloaderPool'].getfloat('freq', 1.0)
        self.response_text_handle = response_text_handle
        self.task_generator = task_generator()
        self.logger = logging.getLogger('DownloaderPool')

    def stop(self):
        self.runningFlag = False

    def run(self):
        self.runningFlag = True
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36'}
        with aiohttp.ClientSession(loop=loop, headers=headers) as session:
            tasks = [
                asyncio.ensure_future(self.request(session))
                for i in range(self.worker_num)
            ]
            loop.run_until_complete(asyncio.wait(tasks))
        loop.close()

    async def request(self, session):
        '''async url request'''
        while self.runningFlag:
            try:
                url = next(self.task_generator)
                if not url:
                    await asyncio.sleep(0.5)
                    continue
                async with session.get(url) as response:
                    if response.status != 200:
                        raise Exception('Unnormal response')
                    self.response_text_handle(await response.text())
            except StopIteration as e:
                pass
            except Exception as e:
                self.logger.error('\n\tURL:"{0}"\n\tError:{1}'.format(url, e))
            finally:
                await asyncio.sleep(self.freq)

        self.logger.debug('A worker stopping!')
