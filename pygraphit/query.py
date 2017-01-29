import gevent
import pygraphit.helper
import pygraphit.node

class ESQuery(object):
	def __init__(self, conditions={}):
		self.conditions = {}
		self.add(conditions)

	@property
	def query_type(self): return "vertices"

	def add(self, conditions):
		for key, value in conditions.items():
			if type(value) is list:
				self.conditions.setdefault(key, []).extend(value)
			elif type(value) is str:
				self.conditions.setdefault(key, []).append(value)
			else: raise TypeError

	def __str__(self):
		def escape_fieldname(string):
			string = [('\\' + c if c in "\\/+-~=\"<>!(){}[]^:&|*?"
					   else c)
					  for c in string]
			if string[0] in ['\\+','\\-']:
				string[0] = string[0][1:]
			return "".join(string)
		def escape_term(string):
			return "".join([('\\' + c if c in "\\/+-~=\"<>!(){}[]^:&|"
					   else c)
					  for c in string])
		def join_set(lst):
			return "(" + " OR ".join([escape_term(it) for it in lst]) + ")"
		return " ".join(
			["{key}:{val}".format(
				key=escape_fieldname(key),
				val=join_set(val)
			)
			 for key, val
			 in self.conditions.items()]
		)

class IDQuery(object):
	def __init__(self, node_ids):
		self.node_ids=[]
		self.add(node_ids)

	@property
	def query_type(self): return "ids"

	def add(self, node_ids):
		if type(node_ids) is list:
			self.node_ids.extend(node_ids)
		elif type(node_ids) is str:
			self.node_ids.append(node_ids)
		else: raise TypeError

	def __str__(self):
		return ",".join(self.node_ids)

class IDNotFoundError(Exception):
	"""Error when retrieving results"""
	def __init__(self, ID):
		self.message="Node {ID} not found!".format(ID=ID)
		self.ID = ID

	def __str__(self):
		return self.message

def QueryResult(graph, query, limit=-1, offset=0, fields=None, concurrent=10, chunksize=10):
	def get_values(ogit_ids):
		data = {"query":",".join(ogit_ids)}
		if fields: data['fields'] = ', '.join(fields)
		return graph.request(
			'POST',
			'/query/' + 'ids',
			data=data
		)['items']

	if type(query) is IDQuery:
		result_ids = query.node_ids
	elif type(query) is ESQuery:
		result_ids = (i['ogit/_id'] for i in graph.request(
			'POST', '/query/' + query.query_type,
			data={
				"query":str(query),
				"fields":'ogit/_id',
				"limit":limit,
				"offset":offset
			})['items'] if 'ogit/_id' in i)
	else: raise NotImplementedError

	if fields == ['ogit/_id']:
		for res in result_ids:
			yield {'ogit/_id':res}

	for curr_slice in pygraphit.helper.chunks(result_ids, chunksize*concurrent):
		jobs = [gevent.spawn(get_values, list(items)) for items in pygraphit.helper.chunks(curr_slice, chunksize)]
		gevent.joinall(jobs)
		for job in jobs:
			for item in job.value:
				if 'error' in item and item['error']['code'] == 404:
					raise pygraphit.node.GraphitNodeError(
						"Node '{nd}' not found!".format(
							nd=item['error']['ogit/_id']))
				yield item
