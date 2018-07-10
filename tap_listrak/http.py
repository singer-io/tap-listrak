import zeep
import sys
import singer
from singer import metrics
from zeep.exceptions import Fault
import backoff

LOGGER = singer.get_logger()

WSDL = "https://webservices.listrak.com/v31/IntegrationService.asmx?wsdl"

def get_client(config):
    client = zeep.Client(wsdl=WSDL)
    elem = client.get_element("{http://webservices.listrak.com/v31/}WSUser")
    headers = elem(UserName=config["username"], Password=config["password"])
    client.set_default_soapheaders([headers])
    return client

def log_retry_attempt(details):
    _, exception, _ = sys.exc_info()
    LOGGER.info(exception)
    LOGGER.info('Caught retryable error after %s tries. Message: %s. Waiting %s more seconds then retrying...',
                details["tries"],
                exception.message,
                details["wait"])

def request(tap_stream_id, service_fn, **kwargs):
    with metrics.http_request_timer(tap_stream_id) as timer:
        try:
            response = service_fn(**kwargs)
            timer.tags[metrics.Tag.http_status_code] = 200
            LOGGER.info("Making request for message %s page %s with start date: %s",
                        kwargs.get('MsgID'), kwargs.get('Page'), kwargs.get('StartDate'))
            return response
        except Fault as e:
            if "404" in str(e.detail):
                LOGGER.info("Encountered a 404 for message: %s", kwargs['MsgID'])
                return None
            raise
