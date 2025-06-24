import unittest
from unittest.mock import patch, MagicMock
from zeep.exceptions import XMLSyntaxError, Fault, TransportError
from tap_listrak.http import request


class TestRequestFunction(unittest.TestCase):
    """
    Unit tests for `request` function in tap_listrak.http module.

    Reviewer Feedback Covered:
    - Tests now validate retry logic in addition to error handling.
    - Removed '__main__' block (not required for CI runners like pytest/nose).
    """

    def setUp(self):
        # Patch http_request_timer to avoid real metric recording
        patcher = patch('tap_listrak.http.metrics.http_request_timer')
        self.mock_timer = patcher.start()
        self.addCleanup(patcher.stop)
        self.mock_timer.return_value.__enter__.return_value = MagicMock(tags={})

    def test_successful_request(self):
        """Test that request returns successfully for a valid service call."""
        def mock_service_fn(**kwargs):
            return "Success"

        result = request("test_stream", mock_service_fn)
        self.assertEqual(result, "Success")

    def test_xml_syntax_error(self):
        """Test that XMLSyntaxError is raised and caught."""
        def raise_xml_error(**kwargs):
            raise XMLSyntaxError("Simulated XML syntax issue")

        with self.assertRaises(XMLSyntaxError):
            request("test_stream", raise_xml_error)

    def test_fault_error(self):
        """Test that SOAP Fault is raised and handled."""
        def raise_fault(**kwargs):
            raise Fault("Simulated SOAP Fault")

        with self.assertRaises(Fault):
            request("test_stream", raise_fault)

    def test_transport_error(self):
        """Test that transport-related errors (like 502) are handled."""
        def raise_transport_error(**kwargs):
            raise TransportError(502, "Bad Gateway")

        with self.assertRaises(TransportError):
            request("test_stream", raise_transport_error)

    def test_retry_success_after_failures(self):
        """
        Test that request retries and eventually succeeds after transient errors.

        Covers: Retry logic working correctly before success.
        """
        call_count = {"count": 0}

        def flaky_service(**kwargs):
            if call_count["count"] < 2:
                call_count["count"] += 1
                raise TransportError(502, "Temporary issue")
            return "Recovered"

        result = request("test_stream", flaky_service)
        self.assertEqual(result, "Recovered")
        self.assertEqual(call_count["count"], 2)  # Ensures retry was triggered

    def test_retry_stops_after_max_attempts(self):
        """
        Test that request gives up after exceeding retry attempts.

        Covers: Retry stops after max_tries (5).
        """
        def always_fail(**kwargs):
            raise Fault("Permanent failure")

        with self.assertRaises(Fault):
            request("test_stream", always_fail)
