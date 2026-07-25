"""Pure-Python cryptographic primitives (no third-party dependencies).

* **Ed25519** public-key signatures (RFC 8032 reference construction) — real
  asymmetric identity: a node's id can be the fingerprint of its public key, so
  authenticity needs no shared secret and no node can impersonate another.
* **ChaCha20** stream cipher (RFC 8439) for confidentiality.

These are the reference constructions: correct and self-contained, but not
constant-time or fast. For production throughput, swap in the optional
``cryptography`` extra; the interfaces here mirror it closely.
"""

from __future__ import annotations

import hashlib
import os
import struct
from typing import Tuple

# -- Ed25519 (RFC 8032 reference) ------------------------------------------

_b = 256
_q = 2 ** 255 - 19
_l = 2 ** 252 + 27742317777372353535851937790883648493


def _H(m: bytes) -> bytes:
    return hashlib.sha512(m).digest()


def _inv(x: int) -> int:
    return pow(x, _q - 2, _q)


_d = -121665 * _inv(121666) % _q
_I = pow(2, (_q - 1) // 4, _q)


def _xrecover(y: int) -> int:
    xx = (y * y - 1) * _inv(_d * y * y + 1)
    x = pow(xx, (_q + 3) // 8, _q)
    if (x * x - xx) % _q != 0:
        x = (x * _I) % _q
    if x % 2 != 0:
        x = _q - x
    return x


_By = 4 * _inv(5) % _q
_Bx = _xrecover(_By)
_B = (_Bx % _q, _By % _q)


def _edwards(P: Tuple[int, int], Q: Tuple[int, int]) -> Tuple[int, int]:
    x1, y1 = P
    x2, y2 = Q
    x3 = (x1 * y2 + x2 * y1) * _inv(1 + _d * x1 * x2 * y1 * y2)
    y3 = (y1 * y2 + x1 * x2) * _inv(1 - _d * x1 * x2 * y1 * y2)
    return (x3 % _q, y3 % _q)


def _scalarmult(P: Tuple[int, int], e: int) -> Tuple[int, int]:
    if e == 0:
        return (0, 1)
    Q = _scalarmult(P, e // 2)
    Q = _edwards(Q, Q)
    if e & 1:
        Q = _edwards(Q, P)
    return Q


def _encodeint(y: int) -> bytes:
    return y.to_bytes(_b // 8, "little")


def _encodepoint(P: Tuple[int, int]) -> bytes:
    x, y = P
    val = y | ((x & 1) << (_b - 1))
    return val.to_bytes(_b // 8, "little")


def _bit(h: bytes, i: int) -> int:
    return (h[i // 8] >> (i % 8)) & 1


def _pure_publickey(sk: bytes) -> bytes:
    """Derive a 32-byte Ed25519 public key from a 32-byte secret seed."""
    h = _H(sk)
    a = 2 ** (_b - 2) + sum(2 ** i * _bit(h, i) for i in range(3, _b - 2))
    A = _scalarmult(_B, a)
    return _encodepoint(A)


def _Hint(m: bytes) -> int:
    h = _H(m)
    return sum(2 ** i * _bit(h, i) for i in range(2 * _b))


def _pure_sign(sk: bytes, pk: bytes, message: bytes) -> bytes:
    """Produce a 64-byte Ed25519 signature over ``message``."""
    h = _H(sk)
    a = 2 ** (_b - 2) + sum(2 ** i * _bit(h, i) for i in range(3, _b - 2))
    r = _Hint(h[_b // 8 : _b // 4] + message)
    R = _scalarmult(_B, r)
    S = (r + _Hint(_encodepoint(R) + pk + message) * a) % _l
    return _encodepoint(R) + _encodeint(S)


def _decodeint(s: bytes) -> int:
    return int.from_bytes(s, "little")


def _decodepoint(s: bytes) -> Tuple[int, int]:
    y = int.from_bytes(s, "little") & ((1 << (_b - 1)) - 1)
    x = _xrecover(y)
    if x & 1 != _bit(s, _b - 1):
        x = _q - x
    P = (x, y)
    if not _isoncurve(P):
        raise ValueError("decoding point that is not on curve")
    return P


def _isoncurve(P: Tuple[int, int]) -> bool:
    x, y = P
    return (-x * x + y * y - 1 - _d * x * x * y * y) % _q == 0


def _pure_verify(pk: bytes, message: bytes, signature: bytes) -> bool:
    """Verify a 64-byte Ed25519 signature; returns ``True`` iff valid."""
    if len(signature) != _b // 4 or len(pk) != _b // 8:
        return False
    try:
        R = _decodepoint(signature[: _b // 8])
        A = _decodepoint(pk)
        S = _decodeint(signature[_b // 8 : _b // 4])
    except (ValueError, Exception):
        return False
    h = _Hint(_encodepoint(R) + pk + message)
    left = _scalarmult(_B, S)
    right = _edwards(R, _scalarmult(A, h))
    return left == right


import contextlib


@contextlib.contextmanager
def _silence_fd(fd: int):
    """Temporarily silence a raw file descriptor (e.g. a native panic's stderr)."""
    saved = os.dup(fd)
    devnull = os.open(os.devnull, os.O_WRONLY)
    os.dup2(devnull, fd)
    os.close(devnull)
    try:
        yield
    finally:
        os.dup2(saved, fd)
        os.close(saved)


def _load_fast_backend():
    """Bind Ed25519 to ``cryptography`` if it is present and functional.

    Ed25519 is deterministic per RFC 8032, so keys and signatures are
    byte-identical across backends — a node on either backend interoperates
    with one on the other. A broken native install can *panic* (writing to the
    raw stderr fd) rather than raising, so we silence fd 2 and probe the backend
    once before trusting it; any failure falls through to the pure-Python path.
    """
    with _silence_fd(2):
        from cryptography.hazmat.primitives.asymmetric.ed25519 import (
            Ed25519PrivateKey,
            Ed25519PublicKey,
        )

        def _pk(sk: bytes) -> bytes:
            return Ed25519PrivateKey.from_private_bytes(sk).public_key().public_bytes_raw()

        def _sign(sk: bytes, pk: bytes, message: bytes) -> bytes:
            return Ed25519PrivateKey.from_private_bytes(sk).sign(message)

        def _verify(pk: bytes, message: bytes, signature: bytes) -> bool:
            try:
                Ed25519PublicKey.from_public_bytes(pk).verify(signature, message)
                return True
            except Exception:
                return False

        t = b"\x00" * 32
        assert _verify(_pk(t), b"x", _sign(t, b"", b"x"))
        return _pk, _sign, _verify


try:  # pragma: no cover - path depends on optional dependency health
    publickey, sign, verify = _load_fast_backend()
    BACKEND = "cryptography"
except (KeyboardInterrupt, SystemExit):  # pragma: no cover
    raise
except BaseException:  # noqa: BLE001 - any failure (incl. native panics) → fallback
    publickey = _pure_publickey
    sign = _pure_sign
    verify = _pure_verify
    BACKEND = "pure-python"


def generate_keypair() -> Tuple[bytes, bytes]:
    """Return a fresh ``(secret_key, public_key)`` pair."""
    sk = os.urandom(32)
    return sk, publickey(sk)


def fingerprint(pk: bytes, length: int = 16) -> str:
    """A short, stable node id derived from a public key."""
    return hashlib.sha256(pk).hexdigest()[:length]


# -- ChaCha20 (RFC 8439) ---------------------------------------------------

def _rotl32(v: int, c: int) -> int:
    v &= 0xFFFFFFFF
    return ((v << c) | (v >> (32 - c))) & 0xFFFFFFFF


def _quarter_round(x, a, b, c, d) -> None:
    x[a] = (x[a] + x[b]) & 0xFFFFFFFF
    x[d] = _rotl32(x[d] ^ x[a], 16)
    x[c] = (x[c] + x[d]) & 0xFFFFFFFF
    x[b] = _rotl32(x[b] ^ x[c], 12)
    x[a] = (x[a] + x[b]) & 0xFFFFFFFF
    x[d] = _rotl32(x[d] ^ x[a], 8)
    x[c] = (x[c] + x[d]) & 0xFFFFFFFF
    x[b] = _rotl32(x[b] ^ x[c], 7)


def _chacha20_block(key: bytes, counter: int, nonce: bytes) -> bytes:
    constants = (0x61707865, 0x3320646E, 0x79622D32, 0x6B206574)
    state = list(constants)
    state += list(struct.unpack("<8I", key))
    state += [counter & 0xFFFFFFFF]
    state += list(struct.unpack("<3I", nonce))
    working = list(state)
    for _ in range(10):  # 20 rounds = 10 double-rounds
        _quarter_round(working, 0, 4, 8, 12)
        _quarter_round(working, 1, 5, 9, 13)
        _quarter_round(working, 2, 6, 10, 14)
        _quarter_round(working, 3, 7, 11, 15)
        _quarter_round(working, 0, 5, 10, 15)
        _quarter_round(working, 1, 6, 11, 12)
        _quarter_round(working, 2, 7, 8, 13)
        _quarter_round(working, 3, 4, 9, 14)
    out = [(working[i] + state[i]) & 0xFFFFFFFF for i in range(16)]
    return struct.pack("<16I", *out)


def chacha20(key: bytes, nonce: bytes, data: bytes, counter: int = 0) -> bytes:
    """Encrypt/decrypt ``data`` with ChaCha20 (the op is its own inverse).

    ``key`` is 32 bytes, ``nonce`` is 12 bytes.
    """
    if len(key) != 32:
        raise ValueError("key must be 32 bytes")
    if len(nonce) != 12:
        raise ValueError("nonce must be 12 bytes")
    out = bytearray()
    for i in range(0, len(data), 64):
        block = _chacha20_block(key, counter + i // 64, nonce)
        chunk = data[i : i + 64]
        out.extend(b ^ block[j] for j, b in enumerate(chunk))
    return bytes(out)
