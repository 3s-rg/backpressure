"""Tests for the Lua script loader."""

import pytest

from backpressure.loader import get_lua_script


def test_get_lua_script_admission_token_buckets_returns_str() -> None:
    """get_lua_script('admission_token_buckets') returns token-bucket admission script."""
    script = get_lua_script("admission_token_buckets")
    assert isinstance(script, str)
    assert "acquire" in script
    assert len(script) > 100


def test_get_lua_script_unknown_raises() -> None:
    """get_lua_script raises ValueError for an unknown script name."""
    with pytest.raises(ValueError, match=r"unknown script"):
        get_lua_script("nonexistent")


def test_get_lua_script_caches_same_object() -> None:
    """Second call returns the same cached string instance."""
    first = get_lua_script("admission_token_buckets")
    second = get_lua_script("admission_token_buckets")
    assert first is second
