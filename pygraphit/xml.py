from lxml import etree as et

class XMLValidateError(Exception):
	"""Error when retrieving results"""
	def __init__(self):
		self.message="XML invalid!"

	def __str__(self):
		return self.message

class XMLValidator(object):
	def __init__(self, xsd):
		xml_schema_doc = et.parse(xsd)
		self.xml_schema = et.XMLSchema(xml_schema_doc)
	def validate(self, xml_doc):
		if self.xml_schema.validate(xml_doc):
			return True
		raise XMLValidateError()

def prettify_xml(string):
	p = et.XMLParser(remove_blank_text=True)
	return et.tostring(et.fromstring(string, p), pretty_print=True)
