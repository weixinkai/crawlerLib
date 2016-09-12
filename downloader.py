# coding:utf-8
import time
import asyncio
import aiohttp
from . import log
from threading import Thread

class DownloaderPool(Thread):
    def __init__(self, task_generator, response_text_handle, num=10,
                 log_path=''):
        Thread.__init__(self)
        self.runningFlag = False
        self.response_text_handle = response_text_handle
        self.worker_num = num
        self.task_generator = task_generator()
        self.logger = log.generate_logger('DownloaderPool', log_path+'downloader.log',
                                             log.ERROR, log.WARNING)

    def stop(self):
        self.runningFlag = False

    def run(self):
        self.runningFlag = True
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36'}
        with aiohttp.ClientSession(loop=loop, headers=headers) as session:
            self.tasks = [
                asyncio.ensure_future(self.request(session, self.task_generator))
                for i in range(self.worker_num)
            ]
            loop.run_until_complete(asyncio.wait(self.tasks))
        loop.close()

    async def request(self, session, task_generator):
        '''async url request'''
        while self.runningFlag:
            try:
                url = next(task_generator)
                if not url: continue

                async with session.get(url) as response:
                    if response.status != 200:
                        raise Exception('Unnormal response')
                    self.response_text_handle(await response.text())
            except StopIteration as e:
                break
            except Exception as e:
                self.logger.error('\n\tURL:"{0}"\n\tError:{1}'.format(url, e))
            finally:
                await asyncio.sleep(1)

        self.logger.debug('A worker stopping!')
