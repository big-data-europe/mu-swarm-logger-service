import aiohttp
from docker import auth
import docker.client
from docker.utils import update_headers, utils
from docker.constants import (
    DEFAULT_TIMEOUT_SECONDS, DEFAULT_USER_AGENT, IS_WINDOWS_PLATFORM,
    DEFAULT_DOCKER_API_VERSION, STREAM_HEADER_SIZE_BYTES, DEFAULT_NUM_POOLS,
    MINIMUM_DOCKER_API_VERSION
)
from functools import partial
import io
import struct


class APIClient(
        aiohttp.ClientSession,
        docker.api.client.BuildApiMixin,
        docker.api.client.ContainerApiMixin,
        docker.api.client.DaemonApiMixin,
        docker.api.client.ExecApiMixin,
        docker.api.client.ImageApiMixin,
        docker.api.client.NetworkApiMixin,
        docker.api.client.PluginApiMixin,
        docker.api.client.SecretApiMixin,
        docker.api.client.ServiceApiMixin,
        docker.api.client.SwarmApiMixin,
        docker.api.client.VolumeApiMixin):
    """
    A low-level client for the Docker Engine API.

    Example:

        >>> import docker
        >>> client = docker.APIClient(base_url='unix://var/run/docker.sock')
        >>> client.version()
        {u'ApiVersion': u'1.24',
         u'Arch': u'amd64',
         u'BuildTime': u'2016-09-27T23:38:15.810178467+00:00',
         u'Experimental': True,
         u'GitCommit': u'45bed2c',
         u'GoVersion': u'go1.6.3',
         u'KernelVersion': u'4.4.22-moby',
         u'Os': u'linux',
         u'Version': u'1.12.2-rc1'}

    Args:
        base_url (str): URL to the Docker server. For example,
            ``unix:///var/run/docker.sock`` or ``tcp://127.0.0.1:1234``.
        version (str): The version of the API to use. Set to ``auto`` to
            automatically detect the server's version. Default: ``1.24``
        timeout (int): Default timeout for API calls, in seconds.
        tls (bool or :py:class:`~docker.tls.TLSConfig`): Enable TLS. Pass
            ``True`` to enable it with default options, or pass a
            :py:class:`~docker.tls.TLSConfig` object to use custom
            configuration.
        user_agent (str): Set a custom user agent for requests to the server.
    """
    def __init__(self, base_url=None, version=None,
                 timeout=DEFAULT_TIMEOUT_SECONDS, tls=False,
                 user_agent=DEFAULT_USER_AGENT, num_pools=DEFAULT_NUM_POOLS,
                 loop=None):
        if tls and not base_url:
            raise TLSParameterError(
                'If using TLS, the base_url argument must be provided.'
            )

        self.base_url = base_url
        self.timeout = timeout
        headers = {}
        headers['User-Agent'] = user_agent

        self._auth_configs = auth.load_config()

        base_url = utils.parse_host(
            base_url, IS_WINDOWS_PLATFORM, tls=bool(tls)
        )
        if base_url.startswith('http+unix://'):
            connector = aiohttp.UnixConnector(
                path=base_url[11:], limit=num_pools, loop=loop)
            self.base_url = 'http+docker://localunixsocket'
        elif base_url.startswith('npipe://'):
            raise NotImplementedError("npipe:// connection not implemented")
        else:
            if isinstance(tls, TLSConfig):
                raise NotImplementedError("Custom TLSConfig not implemented")
            connector = aiohttp.TCPConnector(
                base_url, limit=num_pools, loop=loop)
            self.base_url = base_url

        super(APIClient, self).__init__(
            loop=loop, headers=headers, connector=connector)

        # version detection needs to be after unix adapter mounting
        if version is None:
            self._version = DEFAULT_DOCKER_API_VERSION
        elif isinstance(version, six.string_types):
            if version.lower() == 'auto':
                self._version = self._retrieve_server_version()
            else:
                self._version = version
        else:
            raise DockerException(
                'Version parameter must be a string or None. Found {0}'.format(
                    type(version).__name__
                )
            )
        if utils.compare_version('1.6', self._version) < 0:
            raise NotImplementedError("Stream logs from API < 1.6")
        if utils.version_lt(self._version, MINIMUM_DOCKER_API_VERSION):
            warnings.warn(
                'The minimum API version supported is {}, but you are using '
                'version {}. It is recommended you either upgrade Docker '
                'Engine or use an older version of Docker SDK for '
                'Python.'.format(MINIMUM_DOCKER_API_VERSION, self._version)
            )

    def _url(self, pathfmt, *args, **kwargs):
        for arg in args:
            if not isinstance(arg, six.string_types):
                raise ValueError(
                    'Expected a string but found {0} ({1}) '
                    'instead'.format(arg, type(arg))
                )

        quote_f = partial(six.moves.urllib.parse.quote_plus, safe="/:")
        args = map(quote_f, args)

        if kwargs.get('versioned_api', True):
            return '{0}/v{1}{2}'.format(
                self.base_url, self._version, pathfmt.format(*args)
            )
        else:
            return '{0}{1}'.format(self.base_url, pathfmt.format(*args))

    def _clean_params(self, params):
        if params is None:
            return None
        else:
            return {
                k: v
                for k, v in params.items()
                if v is not None
            }

    def post(self, *args, **kwargs):
        raise RuntimeError("TODO")

    def get(self, url, params=None, stream=None, timeout=None):
        if stream:
            return super(APIClient, self).get(url,
                params=self._clean_params(params), timeout=None)
        else:
            return super(APIClient, self).get(url,
                params=self._clean_params(params), timeout=self.timeout)

    def put(self, *args, **kwargs):
        raise RuntimeError("TODO")

    def delete(self, *args, **kwargs):
        raise RuntimeError("TODO")

    def _set_request_timeout(self, kwargs):
        """Prepare the kwargs for an HTTP request by inserting the timeout
        parameter, if not already present."""
        kwargs.setdefault('timeout', self.timeout)
        return kwargs

    @update_headers
    def _post(self, url, **kwargs):
        return self.post(url, **self._set_request_timeout(kwargs))

    @update_headers
    def _get(self, url, **kwargs):
        return self.get(url, **self._set_request_timeout(kwargs))

    @update_headers
    def _put(self, url, **kwargs):
        return self.put(url, **self._set_request_timeout(kwargs))

    @update_headers
    def _delete(self, url, **kwargs):
        return self.delete(url, **self._set_request_timeout(kwargs))

    async def _raise_for_status(self, response):
        try:
            response.raise_for_status()
        except aiohttp.HttpProcessingError as e:
            raise await create_api_error_from_http_exception(e, response)

    async def _result(self, async_response, json=False, binary=False):
        assert not (json and binary)

        async with async_response as response:
            await self._raise_for_status(response)
            if json:
                return await response.json()
            if binary:
                return await response.read()
            return await response.text()

    async def _stream_helper(self, async_response, decode=False):
        """Generator for data coming from a chunked-encoded HTTP response."""

        if decode:
            async for chunk in json_stream(self._stream_helper(async_response, False)):
                yield chunk
        else:
            async with async_response as response:
                reader = response.content
                while True:
                    # this read call will block until we get a chunk
                    data = await reader.read(1)
                    if not data:
                        break
                    data += await reader.read(io.DEFAULT_BUFFER_SIZE)
                    yield data

    async def _multiplexed_buffer_helper(self, async_response):
        buf = await self._result(async_response, binary=True)
        buf_length = len(buf)
        walker = 0
        while True:
            if buf_length - walker < STREAM_HEADER_SIZE_BYTES:
                break
            header = buf[walker:walker + STREAM_HEADER_SIZE_BYTES]
            _, length = struct.unpack_from('>BxxxL', header)
            start = walker + STREAM_HEADER_SIZE_BYTES
            end = start + length
            walker = end
            yield buf[start:end]

    async def _multiplexed_response_stream_helper(self, async_response):
        async with async_response as response:
            await self._raise_for_status(response)
            reader = response.content
            while True:
                header = await reader.read(STREAM_HEADER_SIZE_BYTES)
                if not header:
                    break
                _, length = struct.unpack('>BxxxL', header)
                if not length:
                    continue
                data = await reader.read(length)
                if not data:
                    break
                yield data

    async def _stream_raw_result(self, async_response):
        ''' Stream result for TTY-enabled container above API 1.6 '''
        async with async_response as response:
            await self._raise_for_status(response)
            async for out in response.content.iter_chunked(1):
                yield out.decode()

    async def _get_result(self, container, stream, async_response):
        cont = await self.inspect_container(container)
        return await self._get_result_tty(stream, async_response, cont['Config']['Tty'])

    async def _get_result_tty(self, stream, async_response, is_tty):
        # We should also use raw streaming (without keep-alives)
        # if we're dealing with a tty-enabled container.
        if is_tty:
            return self._stream_raw_result(async_response) if stream else \
                await self._result(async_response, binary=True)

        sep = six.binary_type()
        if stream:
            return self._multiplexed_response_stream_helper(async_response)
        else:
            return sep.join(
                [x async for x in self._multiplexed_buffer_helper(async_response)]
            )

## docker.utils.json_stream

import json
import json.decoder

from docker.errors import StreamParseError

import six

json_decoder = json.JSONDecoder()


async def stream_as_text(stream):
    """
    Given a stream of bytes or text, if any of the items in the stream
    are bytes convert them to text.
    This function can be removed once we return text streams
    instead of byte streams.
    """
    async for data in stream:
        if not isinstance(data, six.text_type):
            data = data.decode('utf-8', 'replace')
        yield data


def json_splitter(buffer):
    """Attempt to parse a json object from a buffer. If there is at least one
    object, return it and the rest of the buffer, otherwise return None.
    """
    buffer = buffer.strip()
    try:
        obj, index = json_decoder.raw_decode(buffer)
        rest = buffer[json.decoder.WHITESPACE.match(buffer, index).end():]
        return obj, rest
    except ValueError:
        return None


def json_stream(stream):
    """Given a stream of text, return a stream of json objects.
    This handles streams which are inconsistently buffered (some entries may
    be newline delimited, and others are not).
    """
    return split_buffer(stream, json_splitter, json_decoder.decode)


def line_splitter(buffer, separator=u'\n'):
    index = buffer.find(six.text_type(separator))
    if index == -1:
        return None
    return buffer[:index + 1], buffer[index + 1:]


async def split_buffer(stream, splitter=None, decoder=lambda a: a):
    """Given a generator which yields strings and a splitter function,
    joins all input, splits on the separator and yields each chunk.
    Unlike string.split(), each chunk includes the trailing
    separator, except for the last one if none was found on the end
    of the input.
    """
    splitter = splitter or line_splitter
    buffered = six.text_type('')

    async for data in stream_as_text(stream):
        buffered += data
        while True:
            buffer_split = splitter(buffered)
            if buffer_split is None:
                break

            item, buffered = buffer_split
            yield item

    if buffered:
        try:
            yield decoder(buffered)
        except Exception as e:
            raise StreamParseError(e)

## docker.errors

from docker.errors import DockerException


async def create_api_error_from_http_exception(e, response):
    if response.status < 400:
        return
    try:
        explanation = (await response.json())['message']
    except ValueError:
        explanation = (await response.text()).strip()
    cls = APIError
    if response.status == 404:
        if explanation and ('No such image' in str(explanation) or
                            'not found: does not exist or no pull access'
                            in str(explanation)):
            cls = ImageNotFound
        else:
            cls = NotFound
    raise cls(e, response=response, explanation=explanation)


class APIError(aiohttp.HttpProcessingError, DockerException):
    """
    An HTTP error from the API.
    """
    def __init__(self, e, response=None, explanation=None):
        # requests 1.2 supports response as a keyword argument, but
        # requests 1.1 doesn't
        super(APIError, self).__init__(code=e.code, message=e.message)
        self.response = response
        self.explanation = explanation

    def __str__(self):
        message = super(APIError, self).__str__()

        if self.is_client_error():
            message = '{0} Client Error: {1}'.format(
                self.code, self.message)

        elif self.is_server_error():
            message = '{0} Server Error: {1}'.format(
                self.code, self.message)

        if self.explanation:
            message = '{0} ("{1}")'.format(message, self.explanation)

        return message

    def is_client_error(self):
        if self.code is None:
            return False
        return 400 <= self.code < 500

    def is_server_error(self):
        if self.code is None:
            return False
        return 500 <= self.code < 600


class NotFound(APIError):
    pass


class ImageNotFound(NotFound):
    pass



