#coding:utf-8

class BaseAnalyzer:
	def __init__(self, str_content = None):
		self._items = None
		self._urls = None

	@property
	def items(self):
		return self._items

	@property
	def urls(self):
		return self._urls

