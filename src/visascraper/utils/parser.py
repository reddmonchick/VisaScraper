from __future__ import annotations

from bs4 import BeautifulSoup

from visascraper.utils.logger import logger


def safe_get(data: dict, key: str, default: str = "") -> str:
    """Безопасно извлекает значение из словаря и подставляет default для None."""
    value = data.get(key, default)
    return default if value is None else value


def extract_status_batch(html_content: str) -> str:
    """Извлекает текст статуса из HTML-контента для Batch Application."""
    if not html_content:
        return ""
    try:
        soup = BeautifulSoup(html_content, "lxml")
        status_span = soup.find("span")
        return status_span.text.strip() if status_span else ""
    except Exception as exc:
        logger.warning("Не удалось извлечь batch status: %s", exc)
        return ""


def extract_status(html_content: str) -> str:
    """Извлекает текст статуса из HTML-контента."""
    if not html_content:
        return ""
    try:
        soup = BeautifulSoup(html_content, "lxml")
        status_span = soup.find("span")
        return status_span.text.strip() if status_span else ""
    except Exception as exc:
        logger.warning("Не удалось извлечь status: %s", exc)
        return ""


def extract_action_link(html_content: str) -> str:
    """Извлекает ссылку из блока action."""
    if not html_content:
        return ""
    try:
        soup = BeautifulSoup(html_content, "lxml")
        action_link = soup.find("a", class_="btn btn-sm btn-outline-info")
        if action_link and action_link.has_attr("href"):
            return action_link["href"]
        return ""
    except Exception as exc:
        logger.warning("Не удалось извлечь action link: %s", exc)
        return ""


def extract_reg_number(html_content: str) -> str:
    """Извлекает регистрационный номер из HTML."""
    if not html_content:
        return ""
    try:
        soup = BeautifulSoup(html_content, "lxml")
        link = soup.find("a")
        return link.text if link else html_content
    except Exception as exc:
        logger.warning("Не удалось извлечь registration number: %s", exc)
        return html_content


def extract_visa(html_content: str) -> str:
    """Извлекает ссылку на PDF визы."""
    if not html_content:
        return ""
    try:
        soup = BeautifulSoup(html_content, "lxml")
        link = soup.find("a", class_="fw-bold btn btn-sm btn-outline-info btn-back")
        return link["href"] if link and link.has_attr("href") else ""
    except Exception as exc:
        logger.warning("Не удалось извлечь visa link: %s", exc)
        return ""


def extract_detail(html_content: str) -> str:
    if not html_content:
        return ""
    try:
        soup = BeautifulSoup(html_content, "lxml")
        detail_link = soup.find("a", class_="btn btn-sm btn-primary")
        return detail_link["href"] if detail_link and detail_link.has_attr("href") else ""
    except Exception as exc:
        logger.warning("Не удалось извлечь detail link: %s", exc)
        return ""
