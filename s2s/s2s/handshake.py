"""Cooked-mode signature (S2S_Signature C struct)."""

from __future__ import annotations

# struct S2S_Signature {
#   char _signature[128];
#   char _serverName[256];
#   char _mgmtPort[16];
# };
SIG_BANNER_LEN = 128
SIG_SERVER_NAME_LEN = 256
SIG_MGMT_PORT_LEN = 16
SIGNATURE_SIZE = SIG_BANNER_LEN + SIG_SERVER_NAME_LEN + SIG_MGMT_PORT_LEN  # 400

COOKED_BANNER_V2 = b"--splunk-cooked-mode-v2--"
COOKED_BANNER_V3 = b"--splunk-cooked-mode-v3--"
COOKED_BANNER = COOKED_BANNER_V3  # default for fixtures / exports


def parse_signature(buf: bytes) -> tuple[int, str, str] | None:
    """Parse a full 400-byte signature.

    Returns ``(protocol_version, server_name, mgmt_port)`` or ``None`` if
    the buffer is too short or the banner is unrecognized.
    """
    if len(buf) < SIGNATURE_SIZE:
        return None
    banner = buf[:SIG_BANNER_LEN].rstrip(b"\x00")
    if banner == COOKED_BANNER_V3:
        version = 3
    elif banner == COOKED_BANNER_V2:
        version = 2
    else:
        return None
    server = buf[SIG_BANNER_LEN : SIG_BANNER_LEN + SIG_SERVER_NAME_LEN].rstrip(b"\x00")
    port = buf[
        SIG_BANNER_LEN
        + SIG_SERVER_NAME_LEN : SIG_BANNER_LEN
        + SIG_SERVER_NAME_LEN
        + SIG_MGMT_PORT_LEN
    ].rstrip(b"\x00")
    try:
        return version, server.decode("utf-8", errors="replace"), port.decode(
            "utf-8", errors="replace"
        )
    except Exception:
        return version, "", ""


def pack_signature(
    *,
    version: int = 3,
    server_name: str = "s2s-decode",
    mgmt_port: str = "8089",
) -> bytes:
    """Build a 400-byte cooked signature for fixtures / replies."""
    banner = COOKED_BANNER_V3 if version >= 3 else COOKED_BANNER_V2
    sig = bytearray(SIGNATURE_SIZE)
    sig[: len(banner)] = banner
    sn = server_name.encode("utf-8")[: SIG_SERVER_NAME_LEN - 1]
    sig[SIG_BANNER_LEN : SIG_BANNER_LEN + len(sn)] = sn
    mp = mgmt_port.encode("utf-8")[: SIG_MGMT_PORT_LEN - 1]
    base = SIG_BANNER_LEN + SIG_SERVER_NAME_LEN
    sig[base : base + len(mp)] = mp
    return bytes(sig)
