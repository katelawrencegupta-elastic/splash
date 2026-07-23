"""Public API for the Splunk S2S cooked decoder."""

from s2s.decoder import S2SSession, S2SStats
from s2s.framing import SPLK_MAGIC, pack_header, unpack_header
from s2s.handshake import SIGNATURE_SIZE, pack_signature
from s2s.kv import parse_kv_payload, pack_kv_payload
from s2s.message import S2SMessage, encode_message, encode_capabilities_reply
from s2s.normalize import to_logstash_event

__all__ = [
    "S2SSession",
    "S2SStats",
    "S2SMessage",
    "SIGNATURE_SIZE",
    "SPLK_MAGIC",
    "pack_header",
    "unpack_header",
    "pack_signature",
    "encode_message",
    "encode_capabilities_reply",
    "parse_kv_payload",
    "pack_kv_payload",
    "to_logstash_event",
]
