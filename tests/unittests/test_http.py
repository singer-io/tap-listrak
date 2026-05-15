import unittest
from unittest.mock import MagicMock, patch
from zeep.exceptions import XMLSyntaxError, Fault, TransportError
from tap_listrak.http import request

MAX_RETRIES = 5


class TestHttpRequest(unittest.TestCase):

    def setUp(self):
        """Start common patchers before every test."""
        # Patch the http_request_timer context manager
        timer_patcher = patch("tap_listrak.http.metrics.http_request_timer")
        self.mock_http_timer = timer_patcher.start()
        mock_context = MagicMock()
        self.mock_http_timer.return_value.__enter__.return_value = mock_context
        self.addCleanup(timer_patcher.stop)

        # Patch time.sleep so backoff doesn't actually wait
        sleep_patcher = patch("time.sleep", return_value=None)
        self.mock_sleep = sleep_patcher.start()
        self.addCleanup(sleep_patcher.stop)

    def test_successful_request(self):
        """Test a successful SOAP request returns the expected result."""
        def service_fn(**kwargs):
            return "Success"

        result = request("test_stream", service_fn)
        self.assertEqual(result, "Success")
        self.assertEqual(self.mock_http_timer.call_count, 1)

    def test_xml_syntax_error_retry(self):
        """Test that XMLSyntaxError triggers retries up to MAX_RETRIES."""
        failing_mock = MagicMock(side_effect=XMLSyntaxError("Simulated XML error"))

        with self.assertRaises(XMLSyntaxError):
            request("test_stream", failing_mock)

        self.assertEqual(failing_mock.call_count, MAX_RETRIES)
        self.assertEqual(self.mock_http_timer.call_count, MAX_RETRIES)
        self.assertEqual(self.mock_sleep.call_count, MAX_RETRIES - 1)

    def test_fault_error_retry(self):
        """Test that Fault exception triggers retries up to MAX_RETRIES."""
        failing_mock = MagicMock(side_effect=Fault("Simulated Fault"))

        with self.assertRaises(Fault):
            request("test_stream", failing_mock)

        self.assertEqual(failing_mock.call_count, MAX_RETRIES)
        self.assertEqual(self.mock_http_timer.call_count, MAX_RETRIES)
        self.assertEqual(self.mock_sleep.call_count, MAX_RETRIES - 1)

    def test_transport_error_retry(self):
        """Test that TransportError triggers retries up to MAX_RETRIES."""
        failing_mock = MagicMock(side_effect=TransportError(502, "Bad Gateway"))

        with self.assertRaises(TransportError):
            request("test_stream", failing_mock)

        self.assertEqual(failing_mock.call_count, MAX_RETRIES)
        self.assertEqual(self.mock_http_timer.call_count, MAX_RETRIES)
        self.assertEqual(self.mock_sleep.call_count, MAX_RETRIES - 1)

    def test_retry_recovers_before_max_attempts(self):
        """Test that a request recovers after a few retries before reaching max."""
        service_mock = MagicMock()
        service_mock.side_effect = [XMLSyntaxError("fail"), "Recovered"]

        result = request("test_stream", service_mock)

        self.assertEqual(result, "Recovered")
        self.assertEqual(service_mock.call_count, 2)
        self.assertEqual(self.mock_http_timer.call_count, 2)
        self.assertEqual(self.mock_sleep.call_count, 1)

    def test_unhandled_exception_not_retried(self):
        """Test that unexpected exceptions are not retried."""
        service_mock = MagicMock(side_effect=ValueError("unexpected"))

        with self.assertRaises(ValueError):
            request("test_stream", service_mock)

        self.assertEqual(service_mock.call_count, 1)
        self.assertEqual(self.mock_http_timer.call_count, 1)

    def test_none_as_service_fn(self):
        """Test that passing None instead of a function raises TypeError."""
        with self.assertRaises(TypeError):
            request("test_stream", None)

    def test_service_fn_returns_none(self):
        """Test that a service function returning None is handled properly."""
        def service_fn(**kwargs):
            return None

        result = request("test_stream", service_fn)
        self.assertIsNone(result)
        self.assertEqual(self.mock_http_timer.call_count, 1)

    def test_invalid_logon_attempt_not_retried(self):
        """Ensure InvalidLogonAttempt Fault is not retried and raised immediately."""
        failing_mock = MagicMock(side_effect=Fault("InvalidLogonAttempt"))

        with self.assertRaises(Fault) as ctx:
            request("test_stream", failing_mock)

        self.assertIn("InvalidLogonAttempt", str(ctx.exception))
        self.assertEqual(failing_mock.call_count, 1)
        self.assertEqual(self.mock_http_timer.call_count, 1)
