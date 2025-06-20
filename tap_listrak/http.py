import zeep
import sys
import singer
from singer import metrics
from zeep.exceptions import Fault, TransportError, XMLSyntaxError
import backoff

LOGGER = singer.get_logger()

WSDL = "https://webservices.listrak.com/v31/IntegrationService.asmx?wsdl"

def get_client(config):
    """Initialize the SOAP client with authentication headers."""
    client = zeep.Client(wsdl=WSDL)
    elem = client.get_element("{http://webservices.listrak.com/v31/}WSUser")
    headers = elem(UserName=config["username"], Password=config["password"])
    client.set_default_soapheaders([headers])
    return client

def log_retry_attempt(details):
    """Log details about a backoff retry attempt."""
    _, exception, _ = sys.exc_info()
    LOGGER.info("Retryable exception: %s", exception)
    LOGGER.info("Retry attempt %s. Waiting %s seconds before next try...",
                details["tries"], details["wait"])

@backoff.on_exception(
    backoff.expo,
    (XMLSyntaxError, TransportError, Fault),
    max_tries=5,
    jitter=None,
    on_backoff=log_retry_attempt
)
def request(tap_stream_id, service_fn, **kwargs):
    """Make SOAP API request with error handling and retry."""
    with metrics.http_request_timer(tap_stream_id) as timer:
        try:
            response = service_fn(**kwargs)
            timer.tags[metrics.Tag.http_status_code] = 200
            LOGGER.info("Request successful for stream: %s | Page: %s | Start: %s",
                        tap_stream_id, kwargs.get('Page'), kwargs.get('StartDate'))
            return response

        except Exception as e:
            LOGGER.exception("Unhandled exception in stream '%s' : %s", tap_stream_id, str(e))
            raise
