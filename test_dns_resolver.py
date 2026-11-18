"""Tests for direct PTR DNS and fallback, without network access."""
import socket
import struct
import unittest
from unittest.mock import patch

from dns_resolver import ptr_query


class FakeSocket:
    def __init__(self, reply, fail=False):
        self.reply = reply
        self.fail = fail
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass
    def settimeout(self, timeout):
        pass
    def connect(self, address):
        self.address = address
    def send(self, request):
        self.request = request
    def recv(self, size):
        if self.fail:
            raise socket.timeout("unreachable")
        question = self.request[12:]
        header = struct.pack("!HHHHHH", struct.unpack_from("!H", self.request)[0], 0x8180, 1, 1, 0, 0)
        hostname = b"\x04host\x03lan\x00"
        answer = b"\xc0\x0c" + struct.pack("!HHIH", 12, 1, 60, len(hostname)) + hostname
        return header + question + answer


class DirectPTRTests(unittest.TestCase):
    def test_fallback_to_second_server(self):
        calls = []
        def make_socket(*args):
            calls.append(1)
            return FakeSocket(None, fail=len(calls) == 1)
        with patch("dns_resolver.socket.socket", side_effect=make_socket):
            self.assertEqual(ptr_query("192.168.1.1", ["192.0.2.1", "192.0.2.2"], 0.01), "host.lan")
        self.assertEqual(len(calls), 2)

    def test_no_answer_returns_none(self):
        with patch("dns_resolver.socket.socket", return_value=FakeSocket(None, fail=True)):
            self.assertIsNone(ptr_query("192.168.1.1", ["192.0.2.1"], 0.01))

    def test_invalid_ip_is_rejected(self):
        with self.assertRaises(ValueError):
            ptr_query("not-an-ip", ["192.0.2.1"])


if __name__ == "__main__":
    unittest.main()
