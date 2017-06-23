# coding:utf8
import sys, os
lib_dir = os.path.abspath(os.path.dirname(os.getcwd())+os.path.sep+"..")
sys.path.append(lib_dir)

from redis import StrictRedis
from crawlerLib import CrawlerCoordinator
from plugins import SqliteConnector, HtmlAnalyzer

if __name__ == '__main__':
    import time, os
    # if(os.path.exists('data.db')):
    #     os.remove('data.db')

    try:
        items_pipe = SqliteConnector('data.db')
        c = CrawlerCoordinator(analyzer_class=HtmlAnalyzer, put_items_api=items_pipe.put_items)
    except Exception as e:
        print('init error {0}'.format(e))
        sys.exit(1)

    try:
        c.task_init(['http://python.jobbole.com/category/tools/'], True)
        items_pipe.start()
        c.start()
        time.sleep(20)
    except KeyboardInterrupt:
        pass
    finally:
        c.stop()
        items_pipe.stop()
        items_pipe.join()
    input("Enter to exit")
