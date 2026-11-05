import functools

import pytest

from webants.libs.request import Request


async def async_generator_callback(response, **kwargs):
    yield None


async def coroutine_callback(response, **kwargs):
    return None


async def async_generator_errback(error, request):
    yield None


def test_headers_are_empty_by_default():
    request = Request("http://example.com", callback=coroutine_callback)
    assert request.headers == {}


def test_explicit_headers_are_preserved():
    request = Request(
        "http://example.com",
        callback=coroutine_callback,
        headers={"User-Agent": "custom", "X-Test": "1"},
    )
    assert request.headers == {"User-Agent": "custom", "X-Test": "1"}


def test_priority_ordering_is_strict_weak_order():
    high = Request("http://example.com/1", callback=coroutine_callback, priority=1)
    low = Request("http://example.com/2", callback=coroutine_callback, priority=5)
    same = Request("http://example.com/3", callback=coroutine_callback, priority=1)

    assert high < low
    assert not low < high
    assert not high < high
    assert not high < same
    assert not same < high


def test_async_generator_callback_is_accepted():
    request = Request("http://example.com", callback=async_generator_callback)
    assert request.callback is async_generator_callback


def test_coroutine_callback_is_accepted():
    request = Request("http://example.com", callback=coroutine_callback)
    assert request.callback is coroutine_callback


def test_partial_coroutine_callback_is_accepted():
    callback = functools.partial(coroutine_callback)
    request = Request("http://example.com", callback=callback)
    assert request.callback is callback


def test_sync_callback_is_rejected():
    def sync_callback(response, **kwargs):
        return None

    with pytest.raises(TypeError, match="coroutine function or an async generator"):
        Request("http://example.com", callback=sync_callback)


def test_async_generator_errback_is_rejected():
    with pytest.raises(TypeError, match="errback must be a coroutine function"):
        Request("http://example.com", errback=async_generator_errback)


def test_non_callable_callback_is_rejected():
    with pytest.raises(TypeError, match="callback must be callable"):
        Request("http://example.com", callback="not-callable")


def test_reserved_cb_kwargs_key_is_rejected():
    with pytest.raises(TypeError, match="reserved key 'response'"):
        Request(
            "http://example.com",
            callback=coroutine_callback,
            cb_kwargs={"response": "duplicate"},
        )


def test_non_string_cb_kwargs_key_is_rejected():
    with pytest.raises(TypeError, match="cb_kwargs keys must be str"):
        Request(
            "http://example.com",
            callback=coroutine_callback,
            cb_kwargs={1: "value"},
        )


def test_non_dict_cb_kwargs_is_rejected():
    with pytest.raises(TypeError, match="cb_kwargs must be a dict"):
        Request(
            "http://example.com",
            callback=coroutine_callback,
            cb_kwargs=["not", "a", "dict"],
        )
