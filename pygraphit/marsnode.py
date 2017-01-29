import pygraphit.node
import pygraphit.xml
from lxml import etree as et
import hashlib

class MARSNode(pygraphit.node.GraphitNode):
	@classmethod
	def from_xmlfile(cls, session, filename, validator=None):
		try:
			xml_doc = et.parse(filename).getroot()
			if validator:
				validator.validate(xml_doc)
			ogit_id = xml_doc.attrib['ID']
			ogit_name = xml_doc.attrib['NodeName']
			ogit_automation_marsnodetype = xml_doc.attrib['NodeType']
			ogitid = hashlib.md5(ogit_id).hexdigest()
			data = {
				'ogit/Automation/marsNodeFormalRepresentation':et.tostring(xml_doc),
				'ogit/_owner': xml_doc.attrib['CustomerID'],
				'ogit/_id': ogit_id,
				'ogit/_type':'ogit/Automation/MARSNode',
				'ogit/name':ogit_name,
				'ogit/Automation/marsNodeType': ogit_automation_marsnodetype,
				'ogit/id':ogitid
			}
		except pygraphit.xml.XMLValidateError:
			raise MARSNodeError("ERROR: {f} does not contain a valid MARS node".format(f=filename))
		except et.XMLSyntaxError:
			raise MARSNodeError("ERROR: {f} does not contain valid XML".format(f=filename))
		return cls(session, data)

	def print_node(self, stream):
		try:
			print >>stream, pygraphit.xml.prettify_xml(self.data['ogit/Automation/marsNodeFormalRepresentation'])
		except KeyError as e:
			if 'error' in self.data:
				raise MARSNodeError("ERROR: Node '{nd}' {err}".format(
					nd=self.data['error']['ogit/_id'], err=self.data['error']['message']))
			else:
				raise MARSNodeError("ERROR: Node {nd} is missing 'ogit/Automation/marsNodeFormalRepresentation' attribute! Maybe it's not a MARS node?".format(nd=self.data['ogit/_id']))

class MARSNodeError(Exception):
	"""Error when retrieving results"""
	def __init__(self, message):
		self.message=message

	def __str__(self):
		return self.message
