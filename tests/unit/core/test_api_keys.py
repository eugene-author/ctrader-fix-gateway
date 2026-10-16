from app.core.api_keys import generate_api_key, hash_api_key, key_prefix, verify_api_key


def test_verify_api_key_accepts_the_original_key() -> None:
    raw_key = "sk_live_abcdef1234567890"
    hashed = hash_api_key(raw_key, pepper="test-pepper")

    assert verify_api_key(raw_key, hashed, pepper="test-pepper") is True


def test_verify_api_key_rejects_a_wrong_key() -> None:
    hashed = hash_api_key("sk_live_abcdef1234567890", pepper="test-pepper")

    assert verify_api_key("sk_live_wrongwrongwrong", hashed, pepper="test-pepper") is False


def test_verify_api_key_rejects_the_right_key_with_the_wrong_pepper() -> None:
    raw_key = "sk_live_abcdef1234567890"
    hashed = hash_api_key(raw_key, pepper="test-pepper")

    assert verify_api_key(raw_key, hashed, pepper="a-different-pepper") is False


def test_hash_api_key_is_not_the_plaintext() -> None:
    raw_key = "sk_live_abcdef1234567890"

    hashed = hash_api_key(raw_key, pepper="test-pepper")

    assert hashed != raw_key
    assert raw_key not in hashed


def test_key_prefix_is_the_first_eight_characters() -> None:
    assert key_prefix("sk_live_abcdef1234567890") == "sk_live_"


def test_generate_api_key_round_trips_through_hash_and_verify() -> None:
    raw_key = generate_api_key()

    hashed = hash_api_key(raw_key, pepper="test-pepper")

    assert raw_key.startswith("ctg_live_")
    assert verify_api_key(raw_key, hashed, pepper="test-pepper") is True


def test_generate_api_key_is_not_deterministic() -> None:
    assert generate_api_key() != generate_api_key()
