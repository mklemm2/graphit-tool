import requests, time

class WSO2AuthBase(requests.auth.AuthBase):
	def __init__(self, baseurl, verify=True):
		self._baseurl = baseurl
		self._verify = verify
		self.get_token()

	def get_token(self, auth=None, headers=None, post_data=None):
		try:
			r = requests.post(
				"{baseurl}/oauth2/token".format(baseurl=self._baseurl),
				auth=auth, data=post_data, headers=headers,
				verify=self._verify)
			r.raise_for_status()
		except requests.exceptions.HTTPError as e:
			if r.status_code == 401:
				raise WSO2Error("Could not get an access token from WSO2, check client credentials!")
			else:
				raise WSO2Error(e.message)
		except requests.exceptions.ConnectionError as e:
			raise WSO2Error("Could not connect to WSO2: " + str(e))
		self._token = Token(r.json())

	def renew_token(self, *args, **kwargs):
		self.get_token(*args, **kwargs)

	def __str__(self):
		str = "Token {token} expires in {exp} seconds."
		return str.format(
			token=self._token.access_token,
			exp=int(self._token.expires_in))

	@property
	def token(self):
		return self._token.access_token

	def __call__(self, r):
		#self.logger.debug("Inserting OAuth token into request header.")
		r.headers['_TOKEN'] = self._token.access_token
		return r

class WSO2AuthClientCredentials(WSO2AuthBase):
	def __init__(self, baseurl=None, client=None, verify=True):
		self._client_id, self._client_secret = client
		super(WSO2AuthClientCredentials, self).__init__(
			baseurl,
			verify=verify
		)

	def get_token(self):
		super(WSO2AuthClientCredentials, self).get_token(
			auth = requests.auth.HTTPBasicAuth(
				self._client_id, self._client_secret),
			headers = {
				"User-Agent": "PyGraphIT/1.0",
				"Content-Type": "application/x-www-form-urlencoded",
				"charset": "UTF-8"
			},
			post_data = {"grant_type": "client_credentials"},
		)

class Token(object):
	def __init__(self, t):
		self.access_token = t['access_token']
		self.expires_at = t['expires_in'] + time.time()
		if 'refresh_token' in t:
			self.refresh_token = t['refresh_token']

	def __str__(self):
		return self.access_token

	@property
	def expires_in(self):
		return int(self.expires_at - time.time())

class WSO2Error(Exception):
	"""Error when talking to GraphIT"""
	def __init__(self, message):
		self.message=message

	def __str__(self):
		return self.message
