import aiohttp
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
