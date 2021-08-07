import pytest
from unittest import mock
import requests
from erde.op import route

def test_get_retry():
	# normal functioning
	called_urls = []
	def new_get(url, *args, **kwargs):
		called_urls.append(url)
		return len(called_urls) - 1

	results = []
	requested_urls = []
	with mock.patch('requests.get', side_effect=new_get) as mm:
		for i in range(10):
			url = f'http://localhost/{i}'
			requested_urls.append(url)
			results.append(route.get_retry(url, {}))

	assert mm.call_count == 10
	assert called_urls == requested_urls

	# connection timeout once
	ok = 'normal response'
	url = 'http://localhost/123'

	resps = [requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError, ok]

	def err(url, params, *args, **kwargs):
		resp = resps.pop(0)
		if isinstance(resp, type) and issubclass(resp, Exception):
			raise resp('planned exception')
		return resp

	# 10 retries by default, should not raise exception
	with mock.patch('requests.get', side_effect=err) as mm:
		assert route.get_retry(url, {}) == ok

	assert mm.call_count == 3

	resps = [requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError, ok]
	# connection timeout exhausts retries
	with mock.patch('requests.get', side_effect=err) as mm:
		with pytest.raises(requests.exceptions.ConnectionError):
			route.get_retry(url, {}, retries=1)


