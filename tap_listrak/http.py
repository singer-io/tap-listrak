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
    LOGGER.info(exception)
    LOGGER.info('Caught retryable error after %s tries. Message: %s. Waiting %s more seconds then retrying...',
                details["tries"],
                str(exception),
                details["wait"])

# Added backoff retry to handle intermittent 502 errors, invalid XML and SOAP faults
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

        # Catch and retry malformed XML responses (often 502 HTML instead of XML)
        except XMLSyntaxError as e:
            LOGGER.warning("XMLSyntaxError in stream '%s': %s", tap_stream_id, str(e))
            raise

        # Catch SOAP Faults (e.g. operation-specific issues) and retry
        except Fault as e:
            LOGGER.warning("SOAP Fault in stream '%s': %s", tap_stream_id, str(e))
            raise

        # Catch low-level network or HTTP issues (e.g. 502/503)
        except TransportError as e:
            LOGGER.warning("TransportError in stream '%s': %s", tap_stream_id, str(e))
            raise
