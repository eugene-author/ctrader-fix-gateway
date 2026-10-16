"""API key generation, hashing and verification. HMAC-SHA256 keyed with a server-side pepper."""

import hashlib
import hmac
import secrets

KEY_PREFIX_LENGTH = 8


def generate_api_key() -> str:
    """Generate a new raw API key - the only time the caller sees the unhashed value."""
    return f"ctg_live_{secrets.token_urlsafe(32)}"


def hash_api_key(raw_key: str, *, pepper: str) -> str:
    """HMAC-SHA256 a raw API key, keyed with `pepper`, for storage as api_key_hmac."""
    return hmac.new(pepper.encode(), raw_key.encode(), hashlib.sha256).hexdigest()


def verify_api_key(raw_key: str, hashed_key: str, *, pepper: str) -> bool:
    """Verify a raw API key against its stored HMAC, in constant time."""
    return hmac.compare_digest(hash_api_key(raw_key, pepper=pepper), hashed_key)


def key_prefix(raw_key: str) -> str:
    """First `KEY_PREFIX_LENGTH` characters of a raw API key, for indexed lookup."""
    return raw_key[:KEY_PREFIX_LENGTH]
