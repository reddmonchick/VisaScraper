from __future__ import annotations

import json
from pathlib import Path

from bs4 import BeautifulSoup
from curl_cffi import requests

from visascraper.captcha_solver import solve_recaptcha
from visascraper.config import ensure_runtime_dirs, settings
from visascraper.utils.logger import logger

ensure_runtime_dirs()
SESSION_STORE_PATH = settings.session_store_path


def _read_store() -> dict[str, str]:
    if not SESSION_STORE_PATH.exists():
        return {}
    try:
        return json.loads(SESSION_STORE_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        logger.warning("Файл сессий %s поврежден, начинаем с пустого словаря", SESSION_STORE_PATH)
        return {}


def save_value(name: str, value: str) -> None:
    data = _read_store()
    data[name] = value
    SESSION_STORE_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_session(name: str) -> str | None:
    return _read_store().get(name)


def login(session: requests.Session, name: str, password: str) -> str | None:
    try:
        headers = {
            "Host": "evisa.imigrasi.go.id",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Priority": "u=0, i",
        }

        response = session.get("https://evisa.imigrasi.go.id/", headers=headers)
        soup = BeautifulSoup(response.text, "lxml")
        menu = soup.find("ul", class_="buy-button list-inline mb-0 d-none d-sm-block")
        if not menu:
            logger.error("Не удалось найти меню входа на сайте evisa.imigrasi.go.id")
            return None

        menu_token = menu.find("a")["href"]
        response = session.get(f"https://evisa.imigrasi.go.id{menu_token}", headers=headers)
        soup = BeautifulSoup(response.text, "lxml")

        recaptcha_node = soup.find("div", class_="g-recaptcha")
        csrf_input = soup.find("input", attrs={"name": "csrf_token"})
        if not recaptcha_node or not csrf_input:
            logger.error("Не найдены обязательные поля для авторизации аккаунта %s", name)
            return None

        captcha_token = solve_recaptcha(recaptcha_node["data-sitekey"], "https://evisa.imigrasi.go.id/front/login")
        if not captcha_token:
            logger.error("Не удалось получить captcha token для аккаунта %s", name)
            return None

        headers.update(
            {
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": "https://evisa.imigrasi.go.id",
                "Referer": "https://evisa.imigrasi.go.id/front/login",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "same-origin",
                "Sec-Fetch-User": "?1",
            }
        )
        data = {
            "csrf_token": csrf_input["value"],
            "_username": name,
            "_password": password,
            "g-recaptcha-response": captcha_token,
        }

        response = session.post("https://evisa.imigrasi.go.id/front/login", headers=headers, data=data)
        session_id = response.cookies.get("PHPSESSID")
        if session_id:
            save_value(name, session_id)
        return session_id
    except Exception as exc:
        logger.error("Ошибка при логине аккаунта %s: %s", name, exc)
        return None


def check_session(session: requests.Session, session_id: str | None) -> bool:
    if not session_id:
        return False

    cookies = {"PHPSESSID": session_id}
    headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "origin": "https://evisa.imigrasi.go.id",
        "priority": "u=1, i",
        "referer": "https://evisa.imigrasi.go.id/web/applications/batch",
        "sec-ch-ua": '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
    }
    data = {
        "draw": "1",
        "columns[0][data]": "no",
        "columns[0][searchable]": "true",
        "columns[0][orderable]": "true",
        "columns[0][search][value]": "",
        "columns[0][search][regex]": "false",
        "start": "0",
        "length": "1",
        "search[value]": "",
        "search[regex]": "false",
    }

    response = session.post(
        "https://evisa.imigrasi.go.id/web/applications/batch/data",
        cookies=cookies,
        headers=headers,
        data=data,
    )
    return response.status_code == 200
