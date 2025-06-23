import unittest
from unittest.mock import patch, MagicMock
from zeep.exceptions import XMLSyntaxError, Fault, TransportError
from tap_listrak.http import request


class TestRequestFunction(unittest.TestCase):
    """
    Unit tests for `request` function in tap_listrak.http module.
    These tests validate correct error handling and retry behavior.
    """

    def setUp(self):
        # Patch the http_request_timer globally for this test class
        patcher = patch('tap_listrak.http.metrics.http_request_timer')
        self.mock_timer = patcher.start()
        self.addCleanup(patcher.stop)
        self.mock_timer.return_value.__enter__.return_value = MagicMock(tags={})

    def test_successful_request(self):
        """Test that request returns successfully for valid service call."""
        def mock_service_fn(**kwargs):
            return "Success"

        result = request("test_stream", mock_service_fn)
        self.assertEqual(result, "Success")

    def test_xml_syntax_error(self):
        """Test handling of malformed XML error."""
        def raise_xml_error(**kwargs):
            raise XMLSyntaxError("Simulated XML syntax issue")

        with self.assertRaises(XMLSyntaxError):
            request("test_stream", raise_xml_error)

    def test_fault_error(self):
        """Test handling of SOAP Fault errors."""
        def raise_fault(**kwargs):
            raise Fault("Simulated Fault")

        with self.assertRaises(Fault):
            request("test_stream", raise_fault)

    def test_transport_error(self):
        """Test handling of transport-related errors (e.g., 502/503)."""
        def raise_transport_error(**kwargs):
            raise TransportError(502, "Bad Gateway")

        with self.assertRaises(TransportError):
            request("test_stream", raise_transport_error)


if __name__ == '__main__':
    unittest.main()
