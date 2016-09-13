# coding:utf-8
logging_config = {
    'version': 1,
    'formatters': {
        'default': {
            'format': '[%(levelname)s] %(asctime)s - %(name)s: %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'default',
            'level': 'DEBUG',
            'stream': 'ext://sys.stdout'
        },
        'file': {
            'class': 'logging.FileHandler',
            'formatter': 'default',
            'level': 'DEBUG',
            'filename': 'crawler.log',
            'encoding': 'utf8'
        }
    },
    'loggers': {
        'Crawler': {
            'level': 'INFO',
            'propagate': 1
        },
        'SpiderPool': {
            'level': 'ERROR',
            'propagate': 1
        },
        'DownloaderPool': {
            'level': 'ERROR',
            'propagate': 1
        }
    },
    'root': {
        'level': 'DEBUG',
        'handlers': ['console', 'file']
    }
}
