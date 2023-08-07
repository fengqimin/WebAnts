import random
import time
from pathlib import Path
from urllib.parse import urlparse

from playwright.async_api import async_playwright
from playwright.sync_api import sync_playwright

try:
    import ujson as json
except ImportError:
    import json

__all__ = ["async_login_with_playwright", "sync_login_with_playwright", "load_cookies"]


async def async_login_with_playwright(
    login_url: str,
    css_username: str,
    username: str,
    css_password: str,
    password: str,
    css_login_button: str,
    data_path: str | Path | None,
) -> dict:
    async with async_playwright() as p:
        browser = await p.chromium.launch()

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
            " Chrome/106.0.0.0 Safari/537.36"
        )
        # Open new page
        page = await context.new_page()
        # Go to http://108.170.5.99/forum.php
        await page.goto(login_url)
        # Click input[name="username"]
        await page.locator(css_username).click()
        # Fill input[name="username"]
        await page.locator(css_username).fill(username)
        # Click input[name="password"]
        await page.locator(css_password).click()
        # Fill input[name="password"]
        await page.locator(css_password).fill(password)
        # Check input[name="cookietime"]
        await page.locator('input[name="cookietime"]').check()
        # Click button:has-text("登錄")
        await page.locator(css_login_button).click()
        # 延时，否则很可能无法获取全部的cookie
        time.sleep(5)
        await page.wait_for_url(login_url)

        resp = await page.goto("http://108.170.5.99/thread-8096355-1-1.html", timeout=0)

        headers = await resp.request.all_headers()
        # 保存登录后的cookie
        if isinstance(data_path, str):
            data_path = Path(data_path)
        elif data_path is None:
            data_path = Path(".")

        if not data_path.exists():
            data_path.mkdir(parents=True)

        storage = await context.storage_state(path=data_path / "state.json")
        with open(data_path / "cookies.json", "w") as f:
            f.write(json.dumps(storage["cookies"]))

        # ---------------------
        await context.close()
        await browser.close()

        return headers


def sync_login_with_playwright(
    login_url: str,
    login_frame_index: int,
    css_login_frame: str,
    css_username: str,
    username: str,
    css_password: str,
    password: str,
    css_login_check: str,
    css_login_button: str,
    data_path: str | Path | None,
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
    headless: bool = False,
) -> dict:
    """
    :param headless:
    :param user_agent:
    :param css_login_check:
    :param css_login_frame:
    :param login_url:
    :param login_frame_index:
    :param css_username:
    :param username:
    :param css_password:
    :param password:
    :param css_login_button:
    :param data_path:
    :return:
    """
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        context = browser.new_context(user_agent=user_agent)
        # Open new page
        page = context.new_page()
        # Go to http://108.170.5.99/forum.php
        page.goto(login_url)
        # Click login button,
        if css_login_frame:
            page.locator(css_login_frame).click()
            page.wait_for_load_state()
        print()
        # Click input[name="username"]
        page.frames[login_frame_index].locator(css_username).click()
        # Fill input[name="username"]
        time.sleep(random.random() + 1)
        page.frames[login_frame_index].locator(css_username).fill(username)
        # Click input[name="password"]
        page.frames[login_frame_index].locator(css_password).click()
        # Fill input[name="password"]
        time.sleep(random.random() + 1)
        page.frames[login_frame_index].locator(css_password).fill(password)
        # Check
        if css_login_check:
            time.sleep(random.random() + 1)
            page.frames[-1].locator(css_login_check).click()
        # Click button:has-text("登錄")
        time.sleep(random.random() + 1)
        page.frames[login_frame_index].locator(css_login_button).click()

        time.sleep(3 * random.random() + 3)
        # page.wait_for_url(login_url, timeout=10.0)

        resp = page.goto(login_url, wait_until="domcontentloaded")

        # print(resp.all_headers())
        headers = resp.request.all_headers()

        # 保存登录后 stage and cookie
        if isinstance(data_path, str):
            data_path = Path(data_path)
        elif data_path is None:
            data_path = Path(".")

        data_path = data_path / urlparse(login_url).netloc

        if not data_path.exists():
            data_path.mkdir(parents=True)

        context.storage_state(path=data_path / "state.json")
        with open(data_path / "cookies.json", "w") as f:
            json.dump(page.context.cookies(), f)

        # ---------------------
        context.close()
        browser.close()

        for k in list(headers.keys()):
            if k.startswith(":"):
                del headers[k]

        with open(data_path / "headers.json", "w") as f:
            json.dump(headers, f)

        return headers


def load_cookies(data_path: str | Path | None = None) -> dict | None:
    if data_path is None:
        return None

    data_path = Path(data_path)

    try:
        with open(data_path / "cookies.json", "r", encoding="utf-8") as f:
            _cookies = json.load(f)
    except FileNotFoundError:
        return None

    cookies = {}
    for cookie in _cookies:
        name = cookie["name"]
        value = cookie["value"]
        cookies[name] = value
    return cookies
