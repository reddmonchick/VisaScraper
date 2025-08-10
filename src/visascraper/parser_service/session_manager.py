import json
import time
from curl_cffi import requests
from bs4 import BeautifulSoup
from captcha_solver import solve_recaptcha

class SessionManager:
    """Управление HTTP-сессией."""
    def __init__(self):
        self.session = requests.Session()

    def get_session(self) -> requests.Session:
        return self.session

def save_value(name, value):
    try:
        with open("src/data.json", "r") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {}

    data[name] = value

    with open("src/data.json", "w") as f:
        json.dump(data, f, indent=4)

def load_session(name: str) -> str | None:
    try:
        with open("src/data.json", "r") as f:
            data = json.load(f)
            return data.get(name)
    except (FileNotFoundError, json.JSONDecodeError):
        return None

def login(name: str, password: str) -> str | None:
    headers = {
        'Host': 'evisa.imigrasi.go.id',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    }
    session = requests.Session()

    try:
        response = session.get('https://evisa.imigrasi.go.id/front/login', headers=headers, impersonate="chrome110")
        response.raise_for_status()
    except Exception as e:
        print(f"Ошибка при получении страницы логина: {e}")
        return None

    soup = BeautifulSoup(response.text, 'lxml')
    recaptcha_key_element = soup.find('div', class_='g-recaptcha')
    csrf_token_element = soup.find('input', attrs={'name': 'csrf_token'})

    if not recaptcha_key_element or not csrf_token_element:
        print('Не удалось найти reCAPTCHA или CSRF токен на странице.')
        return None

    recaptcha_key = recaptcha_key_element.get('data-sitekey')
    csrf_token = csrf_token_element.get('value')

    if not recaptcha_key:
        print('Ключ reCAPTCHA пуст.')
        return None

    captcha_token = solve_recaptcha(recaptcha_key, 'https://evisa.imigrasi.go.id/front/login')
    if not captcha_token:
        print('Не удалось решить reCAPTCHA.')
        return None

    headers.update({
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'https://evisa.imigrasi.go.id',
        'Referer': 'https://evisa.imigrasi.go.id/front/login',
    })

    data = {
        'csrf_token': csrf_token,
        '_username': name,
        '_password': password,
        'g-recaptcha-response': captcha_token,
    }

    try:
        response = session.post('https://evisa.imigrasi.go.id/front/login', headers=headers, data=data, impersonate="chrome110")
        response.raise_for_status()
    except Exception as e:
        print(f"Ошибка при отправке формы логина: {e}")
        return None

    session_id = response.cookies.get('PHPSESSID')
    if session_id:
        save_value(name, session_id)
        print(f"Успешный логин для {name}. Session ID: {session_id}")
    else:
        print(f"Логин для {name} не удался, PHPSESSID не найден.")

    return session_id

def check_session(session_id: str) -> bool:
    if not session_id:
        return False

    cookies = {'PHPSESSID': session_id}
    headers = {
        'Host': 'evisa.imigrasi.go.id',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'X-Requested-With': 'XMLHttpRequest',
    }

    try:
        response = requests.get(
            'https://evisa.imigrasi.go.id/web/applications/batch',
            cookies=cookies,
            headers=headers,
            allow_redirects=False,
            impersonate="chrome110"
        )
        # Успешная сессия должна вернуть 200 OK и не перенаправлять на страницу логина
        is_ok = response.status_code == 200 and "login" not in response.url
        print(f"Проверка сессии {session_id[:8]}...: {'OK' if is_ok else 'FAIL'} (status: {response.status_code})")
        return is_ok
    except Exception as e:
        print(f"Ошибка при проверке сессии: {e}")
        return False
