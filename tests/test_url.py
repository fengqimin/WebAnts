from webants.utils.url import url_to_path, url_with_host


def test_url_to_path_handles_empty_path():
    assert url_to_path("http://example.com") == ""


def test_url_to_path_keeps_existing_behavior():
    assert url_to_path("http://example.com/a/b") == "a/b"
    assert url_to_path("http://example.com/") == ""


def test_url_with_host_returns_relative_url_unchanged():
    assert url_with_host("/path", "example.org") == "/path"


def test_url_with_host_replaces_hostname():
    assert url_with_host("http://old.example/a", "new.example") == "http://new.example/a"


def test_url_with_host_keeps_url_when_new_host_is_none():
    url = "http://old.example/a"
    assert url_with_host(url, None) == url
