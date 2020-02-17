from collections.abc import Collection, Mapping, Generator
from datetime import datetime
from itertools import count
from os import utime, environ
from os.path import getsize
from pathlib import Path
from plistlib import FMT_BINARY, dumps as plist
from shutil import copyfileobj as stream
from time import mktime
from typing import ClassVar, Iterable, Tuple, Union, Optional, Dict
from urllib.parse import unquote
from weakref import proxy

from bar import Bar
from bson.json_util import dumps
from requests import Session, codes
from requests.auth import HTTPDigestAuth
from uri import URI


def api(path:str):
	"""Associate a path with a given method."""
	
	def inner(fn):
		fn.path = Path(path)
		return fn
	
	return inner



class Drop:
	base: ClassVar[URI] = URI('https://cl.ly/')  # The ID is appended to this as the first path element.
	_storage:str = '{self.uploaded.year}/{self.uploaded.month}/{self.uploaded.day}/{self.id}--{self.slug}--{self.type}--{self.original}'  # Name (format string) to use when saving locally.
	
	id: int  # The internal integer identifier.
	type: str  # The meta-type (bulk grouping) of the uploaded drop.
	slug: str  # The URL slug used to access this drop.
	
	name: str  # Current file name.
	original: str  # Original uploaded file name.
	target: Optional[URI]  # Target URI if a "short link" redirection.
	content: Optional[URI]  # Content URI otherwise.
	
	size: int  # Total file size.
	views: int  # View counter.
	uploaded: datetime  # The date and time of initial upload.
	favourite: bool = False  # Has this been marked as a favourite?
	
	index: Optional[int]
	total: Optional[int]
	
	_data: Dict
	_stats: URI
	
	_json_map: Dict[str, str] = {
			'id': 'id',
			'slug': 'slug',
			'created_at': 'uploaded',
			'item_type': 'type',
			'name': 'name',
			'redirect_url': 'target',
			('file_name', 'name'): 'original',
			'view_counter': 'views',
			('source_url', 'remote_url'): 'content',
			'stats_url': '_stats',
			'content_length': 'size',
			'favourite': 'favourite',
		}
	
	def __repr__(self) -> str:
		return f"Drop({'⚠️ ' if self.favourite else ''}{self.slug}, {self.type}, '{self.original}', size={self.size}, uploaded={self.uploaded.isoformat()})"
	
	def __init__(self, slug, api, json:bool=False) -> None:
		self._api = proxy(api)
		
		if json:
			self._apply(slug)
			return
		
		self.slug = slug
		
		result = api.session.get(self.uri)
		
		if result.status_code != codes.ok:
			raise ValueError(f"Received {result.status_code!s} attempting to retrieve drop metadata.")
		
		self._apply(result.json())
	
	def __class_getitem__(Drop, api) -> Generator:
		"""Fetch an iterator of all available drops over the given authenticated API instance.
		
		Args: page, per_page, type (image, bookmark, text, archive, audio, video, unknown), deleted
		"""
		
		result = api('/v3/items').json()
		
		#__import__('pudb').set_trace()
		
		counter = count()
		
		while result.get('links', {}).get('next_url', {}).get('href', None):
			for record in result['data']:
				try:
					drop = Drop(record['slug'], api)
				except ValueError:
					yield record
					continue
				
				drop.index = next(counter)
				drop.total = result['meta']['count']
				yield drop
			
			result = api.session.get(result['links']['next_url']['href']).json()
	
	@property
	def uri(self) -> URI:
		return self.base / self.slug
	
	def save(self, path:Optional[Path]=None):
		if not path:
			path = self._storage.format(self=self)
		
		target = Path(path).absolute()
		target.parent.mkdir(parents=True, exist_ok=True)
		
		# First, write out the .info.json for this drop.
		with target.with_suffix('.info.json').open('w', encoding='utf-8') as out:
			out.write(dumps(self._data, indent=4, sort_keys=True))
		
		if self.type == 'bookmark':
			target = target.with_suffix('.webloc')
			target.write_bytes(plist({'URL': self.target}, fmt=FMT_BINARY))
		
		else:
			if not target.exists() or (self.size and getsize(target) != self.size):
				with target.open('wb', buffering=8192) as out:
					with self._api.session.get(self.content, stream=True) as req:
						stream(req.raw, out)
		
		uploaded = mktime(self.uploaded.timetuple())
		utime(target, (uploaded, uploaded))
	
	def _apply(self, metadata) -> None:
		self._process(metadata)
		self._data = metadata
		
		for origin, destination in self._json_map.items():
			if isinstance(origin, tuple):
				origins = origin
			else:
				origins = (origin, )
			
			for origin in origins:
				if metadata.get(origin, None) is not None:
					setattr(self, destination, metadata.get(origin))
					break
			else:
				setattr(self, destination, None)
	
	def _process(self, data:dict) -> None:
		"""Perform minor additional typecasting or cleanup work after retrieval of a drop's metadata."""
		
		for key, value in data.items():
			if not isinstance(value, str): continue
			
			if key in ('file_name', ):
				value = unquote(value)
			elif key.endswith('_at') and value:
				try:
					value = datetime.strptime(value.rstrip('Z'), '%Y-%m-%dT%H:%M:%S')  # Try with optional trailing Z.
				except ValueError:
					value = datetime.strptime(value, '%Y-%m-%d')  # Attempt without time component.
			
			data[key] = value


class CloudAppClient:
	base: ClassVar[URI] = URI('https://my.cl.ly')
	serialization: ClassVar[str] = "application/json"  # Used for Accept and Content-Type headers.
	
	session: Session
	
	def __init__(self) -> None:
		"""Initialize the client interface."""
		
		super().__init__()
		
		self.session = Session()
		
		self.session.headers['User-Agent'] = 'Ruby.CloudApp.API'
		self.session.headers['Accept'] = self.serialization
		self.session.headers['Content-Type'] = self.serialization
		
		if 'CLOUDAPP_USER' in environ:
			self.authenticate(environ['CLOUDAPP_USER'], environ['CLOUDAPP_PASSWORD'])
	
	def authenticate(self, email:str, password:str): # -> CloudAppClient:
		"""Preserve authentication credentials for later use by RPC calls."""
		
		self.session.headers.pop('Authorization', None)
		self.session.auth = HTTPDigestAuth(email, password)
		
		return self
	
	# Internal Mechanisms
	
	def __call__(self, path:str, method='get', **params):
		"""Issue an API call."""
		uri: URI = self.base / path
		return self.session.request(method, uri, params)
	
	def __getitem__(self, slug:str) -> Drop:
		"""Retrieve a Drop by slug."""
		
		return Drop(slug, self)
	
	def __iter__(self) -> Iterable[Drop]:
		"""Iterate all known drops."""
		
		return Drop[self]
	
	def _parse_errors(self, result):
		if isinstance(result, Mapping):
			return [f"{k}: {v}" for k, v in result.items()]
		
		if isinstance(result, str):
			return [result]
		
		if isinstance(result, Collection):
			return result
		
		return []
