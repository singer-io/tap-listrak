import zeep
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
    """Log retry attempt with clean message."""
    exception = details["exception"]
    LOGGER.warning(
        "Retrying due to error (attempt %s): %s. Waiting %s seconds before next try...",
        details["tries"], str(exception), details["wait"]
    )

# Robust retry handler for common SOAP failure types
@backoff.on_exception(
    backoff.expo,
    (XMLSyntaxError, TransportError, Fault),
    max_tries=5,
    jitter=None,
    on_backoff=log_retry_attempt
)
def request(tap_stream_id, service_fn, **kwargs):
    """Make SOAP API request with retry and structured error logging."""
    with metrics.http_request_timer(tap_stream_id) as timer:
        try:
            response = service_fn(**kwargs)
            timer.tags[metrics.Tag.http_status_code] = 200
            LOGGER.info("Request successful for stream: %s | Page: %s | Start: %s",
                        tap_stream_id, kwargs.get('Page'), kwargs.get('StartDate'))
            return response

        except XMLSyntaxError as e:
            LOGGER.exception("Malformed XML in stream '%s': %s", tap_stream_id, str(e))
            raise

        except Fault as e:
            LOGGER.exception("SOAP Fault in stream '%s': %s", tap_stream_id, str(e))
            raise

        except TransportError as e:
            LOGGER.exception("Transport error in stream '%s': %s", tap_stream_id, str(e))
            raise

        except Exception as e:
            LOGGER.exception("Unexpected exception in stream '%s': %s", tap_stream_id, str(e))
            raise
