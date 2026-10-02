import zeep
import singer
from singer import metrics
from zeep.exceptions import Fault, TransportError, XMLSyntaxError
import backoff

LOGGER = singer.get_logger()

WSDL = "https://webservices.listrak.com/v31/IntegrationService.asmx?wsdl"


class ListrakForbiddenError(Exception):
    """Raised when SOAP credentials lack read access to a Listrak stream."""


def get_client(config):
    client = zeep.Client(wsdl=WSDL)
    elem = client.get_element("{http://webservices.listrak.com/v31/}WSUser")
    headers = elem(UserName=config["username"], Password=config["password"])
    client.set_default_soapheaders([headers])
    return client

def log_retry_attempt(details):
    """Log details about a backoff retry attempt."""
    exception = details.get("exception")
    LOGGER.warning(
        "Retry attempt %s due to error: %s. Waiting %s more seconds before retrying...",
        details["tries"],
        str(exception),
        details["wait"]
    )

def is_non_retriable_exception(exc):
    """Avoid retrying on InvalidLogonAttempt errors."""
    return isinstance(exc, Fault) and "InvalidLogonAttempt" in str(exc)


def is_authorization_fault(exc):
    """
    Identify SOAP faults that indicate the credentials genuinely lack access
    (e.g. InvalidLogonAttempt), as opposed to transient/operational faults.

    This is the single source of truth callers should use to decide whether a
    Fault represents "no access" (safe to exclude a stream from the catalog)
    versus an operational failure that should instead be retried by `request`
    and, if it persists, propagated rather than silently pruning streams.
    """
    return is_non_retriable_exception(exc)

@backoff.on_exception(
    backoff.expo,
    (XMLSyntaxError, TransportError, Fault),
    max_tries=5,
    jitter=None,
    on_backoff=log_retry_attempt,
    giveup=is_non_retriable_exception
)
def request(tap_stream_id, service_fn, **kwargs):
    """Make SOAP API request with retry, metrics, and centralized error logging."""
    with metrics.http_request_timer(tap_stream_id) as timer:
        response = service_fn(**kwargs)
        timer.tags[metrics.Tag.http_status_code] = 200
        LOGGER.info(
            "Request successful for stream: %s | Page: %s | Start: %s",
            tap_stream_id,
            kwargs.get('Page', 'N/A'),
            kwargs.get('StartDate', 'N/A')
        )
        return response
