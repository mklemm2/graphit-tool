import json
from urllib import quote_plus
import requests
from pygraphit import QueryResult

class GraphitSession(requests.Session):

	def __init__(self, baseurl, *args, **kwargs):
		self._baseurl=baseurl
		self._headers = {
			"User-Agent": "PyGraphIT/1.0",
			"charset": "UTF-8"
		}
		super(GraphitSession, self).__init__(*args, **kwargs)

	def request(self, method, url, params=None, data=None):
		try:
			headers = self._headers
			headers["Accept"] = "application/json"
			r = super(GraphitSession, self).request(
				method, self._baseurl + url, headers=self._headers,
				params=params, data=json.dumps(data))
			r.raise_for_status()
		except requests.exceptions.HTTPError as e:
			raise GraphitError(self, r.status_code, e)
		except requests.exceptions.ConnectionError as e:
			raise GraphitError(self, 0, e)
		return r.json()

	def get(self, resource):
		return self.request('GET', resource)
	def update(self, resource, data):
		return self.request('POST', resource, data=data)
	def replace(self, resource, data, params=None):
		return self.request('PUT', resource, data=data, params=params)
	def delete(self, resource):
		return self.request('DELETE', resource)
	def create(self, ogit_type, data):
		return self.request(
			'POST', '/new/' + quote_plus(ogit_type), data=data)
	def query(self, query, limit=-1,
			   offset=0, fields=None, concurrent=10, chunksize=100):
		return QueryResult(self, query,
					 limit=limit, offset=offset, fields=fields,
					 concurrent=concurrent, chunksize=chunksize)

	def __str__(self):
		return 'GraphIT at {url}'.format(url=self._baseurl)


class GraphitError(Exception):
	"""Error when talking to GraphIT"""
	def __init__(self, session, status, error):
		self.status=status
		self.message="{sess} returned an error: {err}".format(
			sess=session,
			err=error)

	def __str__(self):
		return self.message
