import pygraphit.session

class GraphitNode(object):
	def __init__(self, session, data):
		try:
			self.ogit_id = data['ogit/_id']
			self.ogit_type = data['ogit/_type']
			self.data = data
			self.session=session
		except KeyError:
			raise GraphitNodeError("Data invalid, ogit/_id is missing or ogit/_type missing")

	def push(self):
		self.session.replace('/' + self.ogit_id, self.data, params={'createIfNotExists':'true', 'ogit/_type':self.ogit_type})

	def delete(self):
		try:
			self.session.delete('/' + self.ogit_id)
		except pygraphit.session.GraphitError as e:
			if e.status == 404:
				raise GraphitNodeError("Cannot delete node '{nd}': Not found!".format(nd=self.ogit_id))
			elif e.status == 409:
				raise GraphitNodeError("Cannot delete node '{nd}': Already deleted!".format(nd=self.ogit_id))
			else:
				raise GraphitNodeError("Cannot delete node '{nd}': {err}".format(nd=self.ogit_id, err=e))

class GraphitNodeError(Exception):
	"""Error when retrieving results"""
	def __init__(self, message):
		self.message=message

	def __str__(self):
		return self.message
