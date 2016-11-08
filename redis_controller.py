# coding:utf-8
from redis import StrictRedis


class RedisController:
    def __init__(self, config):
        self.isGenerateURL = False

        redis_config = config['Redis']
        host = redis_config.get('host', '127.0.0.1')
        db = redis_config.getint('db', 0)
        port = redis_config.getint('port', 6379)
        self.url_db = StrictRedis(host=host, port=port, db=db)

        self.task_url_name = redis_config.get('task_url_name', 'TaskURLs')
        self.seen_url_name = redis_config.get('seen_url_name', 'SeenURLs')

    def start_url_generator(self):
        self.isGenerateURL = True

    def stop_url_generator(self):
        self.isGenerateURL = False

    def storage_urls(self, urls):
        '''storage urls extracted by spider'''
        if not urls: return

        pipe = self.url_db.pipeline()
        for url in urls:
            pipe.sadd(self.seen_url_name, url)
        flags = pipe.execute()
        if not 1 in flags: return

        #storage new urls to task list
        new_urls = filter(lambda x: x, (url if flag else False
                                        for flag, url in zip(flags, urls)))
        pipe.rpush(self.task_url_name, *new_urls)
        pipe.execute()

    def url_generator(self):
        '''get url from url db for downloader'''
        while self.isGenerateURL:
            url = self.url_db.lpop(self.task_url_name)
            yield url.decode('utf-8') if url else None

    def is_task_empty(self):
        if self.url_db.llen(self.task_url_name) > 0:
            return False
        else:
            return True

    def flushdb(self):
        self.url_db.flushdb()

    def check_connect(self):
        #test redis connection
        self.url_db.ping()

    @property
    def seen_count(self):
        return self.url_db.scard(self.seen_url_name)

    @property
    def task_count(self):
        return self.url_db.llen(self.task_url_name)