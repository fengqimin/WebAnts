import asyncio
import pytest
import httpx

from webants.downloader import Downloader
from webants.libs.request import Request
from unittest.mock import AsyncMock
from unittest.mock import patch


# 修改fixture作用域为function（推荐）
@pytest.fixture(scope="function")
def downloader():
    """创建下载器测试实例"""
    dl = Downloader(concurrency=2, log_level=10)
    yield dl


@pytest.fixture
def mock_response():
    """模拟正常响应"""
    response = httpx.Response(200, content=b"test data")
    return response


@pytest.mark.asyncio
async def test_normal_request(downloader):
    """测试正常请求流程"""

    req = Request(url="https://www.httpbin.org/get?name=downloader", method="GET")
    resp = await downloader.fetch(req)

    assert resp.status_code == 200
    assert resp.json()["args"]["name"] == "downloader"
    assert downloader.stats["successful_requests"] == 1
    await downloader.close()


@pytest.mark.asyncio
async def test_retry_mechanism(downloader):
    """测试超时重试机制"""
    req = Request(url="http://retry.com", method="GET", retries=3)

    # 模拟第一次请求超时，后续成功
    mock = AsyncMock()
    mock.side_effect = [
        httpx.RequestError("Timeout"),
        httpx.Response(200, content=b"retry success"),
    ]

    with patch.object(httpx.AsyncClient, "request", new=mock):
        await downloader.fetch_retry(req)
        await downloader.fetch_retry(req)  # 模拟第二次请求

        assert req.retries == 2  # 剩余重试次数
        assert downloader.stats["retry_requests"] == 1
        assert downloader.stats["total_retries"] == 1


@pytest.mark.asyncio
async def test_http_status_retry(downloader):
    """测试HTTP状态码触发的重试"""
    req = Request(url="http://status.com", method="GET", retries=2)

    # 模拟服务端错误响应
    error_response = httpx.Response(500)
    success_response = httpx.Response(200)

    mock = AsyncMock(side_effect=[error_response, error_response, success_response])

    with patch.object(httpx.AsyncClient, "request", new=mock):
        await downloader.fetch_retry(req)  # 第一次失败
        await downloader.fetch_retry(req)  # 第二次失败
        result = await downloader.fetch_retry(req)  # 成功

        assert result.status_code == 200
        assert downloader.stats["total_retries"] == 2


@pytest.mark.asyncio
async def test_callback_handling(downloader, mock_response):
    """测试回调函数处理"""

    async def test_callback(response, kwargs):
        resp = response
        resp.headers.update(kwargs)
        return resp

    req = Request(
        url="http://callback.com",
        method="GET",
        callback=test_callback,
        cb_kwargs={"X-Test": "true"},
    )

    with patch.object(
        httpx.AsyncClient, "request", new=AsyncMock(return_value=mock_response)
    ):
        resp = await downloader.fetch(req)

        assert resp.headers["X-Test"] == "true"
        assert downloader.stats["successful_requests"] == 1


@pytest.mark.asyncio
async def test_statistics_accuracy(downloader):
    """验证统计系统准确性"""
    requests = [
        Request(url=f"http://stat-test.com/{i}", method="GET", delay=0.1)
        for i in range(5)
    ]

    with patch.object(
        httpx.AsyncClient, "request", new=AsyncMock(return_value=httpx.Response(200))
    ):
        for req in requests:
            await downloader.fetch(req)

    assert downloader.stats["total_requests"] == 5
    assert downloader.stats["successful_requests"] == 5
    assert downloader.stats["min_response_time"] > 0
    assert downloader.stats["max_response_time"] <= downloader.timeout.connect


@pytest.mark.asyncio
async def test_concurrency_control(downloader):
    """测试并发控制有效性"""
    sem = downloader.sem
    initial_value = sem._value

    async with sem:
        assert sem._value == initial_value - 1
        await asyncio.sleep(0.1)

    assert sem._value == initial_value


@pytest.mark.asyncio
async def test_callback_exception_handling(downloader, mock_response):
    """测试回调函数异常处理"""

    async def faulty_callback(response, kwargs):
        raise ValueError("Simulated callback error")

    req = Request(
        url="http://callback-error.com",
        method="GET",
        callback=faulty_callback,
        retries=0,
    )

    with patch.object(
        downloader, "_fetch", new=AsyncMock(return_value=req)
    ):
        await downloader.fetch_retry(req)

        assert downloader.stats["failed_requests"] == 1


@pytest.mark.parametrize("status_code,expected_retries", [(429, 3), (500, 3), (200, 0)])
@pytest.mark.asyncio
async def test_retry_strategies(downloader, status_code, expected_retries):
    """参数化测试不同状态码的重试策略"""
    req = Request(url=f"http://status-{status_code}.com", method="GET")

    mock = AsyncMock(return_value=httpx.Response(status_code))
    with patch.object(httpx.AsyncClient, "request", new=mock):
        result = await downloader.fetch_retry(req)

    if status_code in Downloader.RETRY_CODES:
        assert req.retries == 5 - expected_retries
    else:
        assert result.status_code == status_code
