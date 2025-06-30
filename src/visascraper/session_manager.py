import json
import time
from curl_cffi import requests
from bs4 import BeautifulSoup
from captcha_solver import solve_recaptcha

def save_value(name, value):
    try:
        with open("src/data.json", "r") as f:
            data = json.load(f)
            data[name] = value
    except FileNotFoundError:
        return None
    
    with open("src/data.json", "w") as f:
        json.dump(data, f)

def load_session(name: str) -> str | None:
    try:
        with open("src/data.json", "r") as f:
            data = json.load(f)
            return data.get(name)
    except FileNotFoundError:
        return None

def login(name: str, password: str) -> str | None:
    headers = {
        'Host': 'evisa.imigrasi.go.id',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Priority': 'u=0, i',
    }
    session = requests.Session()

    response = session.get('https://evisa.imigrasi.go.id/front/login',  headers=headers)

    soup = BeautifulSoup(response.text, 'lxml')
    recaptcha_key = soup.find('div', class_='g-recaptcha')['data-sitekey']
    csrf_token = soup.find('input', attrs={'name': 'csrf_token'})['value']

    if not recaptcha_key:
        print('Не нашли на сайте капчу, заканчиваем цикл')
        return None

    captcha_token = solve_recaptcha(recaptcha_key, 'https://evisa.imigrasi.go.id/front/login') 

    if not captcha_token:
        return None

    headers.update({
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'https://evisa.imigrasi.go.id', 
        'Referer': 'https://evisa.imigrasi.go.id/front/login', 
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
    })

    data = {
        'csrf_token': csrf_token,
        '_username': name,
        '_password': password,
        'g-recaptcha-response': captcha_token,
    }

    response = session.post('https://evisa.imigrasi.go.id/front/login',  headers=headers, data=data)
    session_id = response.cookies.get('PHPSESSID')
    save_value(name, session_id)
    return session_id

def check_session(session_id: str) -> bool:
    cookies = {'PHPSESSID': session_id}
    headers = {
        'Host': 'evisa.imigrasi.go.id',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.0',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'X-Requested-With': 'XMLHttpRequest',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'Priority': 'u=0',
    }
    data = {
        'draw': '1',
        'columns[0][data]': 'no',
        'columns[0][searchable]': 'true',
        'columns[0][orderable]': 'true',
        'columns[0][search][value]': '',
        'columns[0][search][regex]': 'false',
        'start': '0',
        'length': '1',
        'search[value]': '',
        'search[regex]': 'false',
    }

    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'origin': 'https://evisa.imigrasi.go.id',
        'priority': 'u=1, i',
        'referer': 'https://evisa.imigrasi.go.id/web/applications/batch?token=21ecbdd65f0f5eaf895b963eb6e5.u7OFBDYga0RZB-dW_87YaAqbl4OloQqxA_Uzb6t0Hl4.yca0XnpyCA8scaAjh4mXKm2t0-n00XncaJRbCpg3RHPIw9pdXH8aIw5RhA',
        'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
        'x-dtpc': '1$268330095_123h4vQHBEJLCLWMROBONSPARBUFCFBUOUCTJE-0e0',
        'x-requested-with': 'XMLHttpRequest',
        # 'Cookie': 'PHPSESSID=pc9ba4o2l1k4moftfo1c602415; dtCookieqlpedpe2=v_4_srv_1_sn_CC87B249DF2919940E5B7C5471E68CE4_perc_100000_ol_0_mul_1_app-3A93092c04db681869_0; _ga=GA1.1.333052217.1750468330; _ga_RLK0ZH5KF3=GS2.1.s1750468330$o1$g0$t1750468330$j60$l0$h0; PHPSESSID=pc9ba4o2l1k4moftfo1c602415',
    }

    response = requests.post(
        'https://evisa.imigrasi.go.id/web/applications/batch/data', 
        cookies=cookies,
        headers=headers,
        data=data,
    )
    return response.status_code == 200

