from python_rucaptcha.re_captcha import ReCaptcha
import time
from curl_cffi import requests
from bs4 import BeautifulSoup
import lxml
import json
import gspread

RUCAPTCHA_KEY = 'bd9c67bafe49a8410846e953fd04ff49'


def safe_get(data: dict, key: str) -> str:
    """Безопасно извлекает значение из словаря, возвращая '' если ключ отсутствует или значение None."""
    return data.get(key) if data.get(key) is not None else ''

def extract_status(html_content: str) -> str:
    """Извлекает текст статуса из HTML-контента."""
    if not html_content:
        return ''
    try:
        soup = BeautifulSoup(html_content, 'lxml')
        status_span = soup.find('span')
        return status_span.text.strip() if status_span else ''
    except Exception as e:
        print(f"Error extracting status: {e}")
        return ''

def extract_action_link(html_content: str) -> str:
    """Извлекает ссылку из action-HTML, добавляя базовый URL, если ссылка найдена."""
    base_url = "https://evisa.imigrasi.go.id" 
    if not html_content:
        return ''
    try:
        soup = BeautifulSoup(html_content, 'lxml')
        action_link = soup.find('a', class_='btn btn-sm btn-outline-info')
        if action_link and action_link.has_attr('href'):
            return f"{base_url}{action_link['href']}"
        return ''
    except Exception as e:
        print(f"Error extracting action link: {e}")
        return ''
    

def extract_reg_number(html_content: str) -> str:
    """Извлекает номер регистрации"""
    try:
        reg_number = BeautifulSoup(html_content, 'lxml')
        number = reg_number.find('a').text
        return number
    except:
        return html_content

def save_value(name, value):
    with open("src/data.json", "w") as f:
        json.dump({name: value}, f)

def load_value(name):
    try:
        with open("src/data.json", "r") as f:
            data = json.load(f)
            return data.get(name)
    except FileNotFoundError:
        return None


def login(name, password):
    session = requests.Session()
    headers = {
        'Host': 'evisa.imigrasi.go.id',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        # 'Accept-Encoding': 'gzip, deflate',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Priority': 'u=0, i',
        # Requests doesn't support trailers
        # 'Te': 'trailers',
        # 'Cookie': '_ga_RLK0ZH5KF3=GS2.1.s1749006893$o3$g1$t1749007915$j52$l0$h0; _ga=GA1.1.802994217.1748517355; PHPSESSID=88c70irka1brlloog0mptcnd62; dtCookieqlpedpe2=v_4_srv_1_sn_F9313A0FC5C47A3E235016B65A7F4D47_perc_100000_ol_0_mul_1_app-3A93092c04db681869_0',
    }

    response = session.get('https://evisa.imigrasi.go.id/front/login',headers=headers)
    print('Начальный сайт с капчей', response)

    soup = BeautifulSoup(response.text, 'lxml')

    recaptcha_key = soup.find('div', class_='g-recaptcha')['data-sitekey']
    csrf_token = soup.find('input', attrs={'name':'csrf_token'})['value']

    if recaptcha_key:
        print('Нашли капчу... Решаем ее')
    else:
        print('Не нашли на сайте капчу, заканчиваем  цикл')
        return '0'



    # Решаем reCAPTCHA
    result = ReCaptcha(rucaptcha_key=RUCAPTCHA_KEY,
        websiteKey=recaptcha_key,
        websiteURL='https://evisa.imigrasi.go.id/front/login'
    ).captcha_handler()


    if result.get('solution') is None:
        print("Ошибка решения капчи:", result.get('errorDescription'))
    else:
        captcha_token = result.get('solution', {}).get('gRecaptchaResponse')
        print("Решение капчи:", captcha_token)

        headers = {
            'Host': 'evisa.imigrasi.go.id',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
            # 'Accept-Encoding': 'gzip, deflate',
            'Content-Type': 'application/x-www-form-urlencoded',
            # 'Content-Length': '1185',
            'Origin': 'https://evisa.imigrasi.go.id',
            'Referer': 'https://evisa.imigrasi.go.id/front/login',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Priority': 'u=0, i',
            # Requests doesn't support trailers
            # 'Te': 'trailers',
            # 'Cookie': '_ga_RLK0ZH5KF3=GS2.1.s1749011998$o4$g1$t1749012146$j59$l0$h0; _ga=GA1.1.802994217.1748517355; dtCookieqlpedpe2=v_4_srv_1_sn_F9313A0FC5C47A3E235016B65A7F4D47_perc_100000_ol_0_mul_1_app-3A93092c04db681869_0; PHPSESSID=lhmc9sp82ampq88iptqchvopl5',
        }

        data = {
            'csrf_token': csrf_token,
            '_username': name,
            '_password': password,
            'g-recaptcha-response': captcha_token,
        }

        response = session.post('https://evisa.imigrasi.go.id/front/login',headers=headers, data=data)

        print(response)
        session_id = response.cookies.get('PHPSESSID')
        save_value(name, session_id)
        return session_id
    

def check_session(session_id: str) -> bool:
    cookies = {
    'PHPSESSID': session_id,
    }

    headers = {
        'Host': 'evisa.imigrasi.go.id',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.0',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        # 'Accept-Encoding': 'gzip, deflate',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'X-Requested-With': 'XMLHttpRequest',
        # 'Content-Length': '2221',
        'Origin': 'https://evisa.imigrasi.go.id',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'Priority': 'u=0',
        # Requests doesn't support trailers
        # 'Te': 'trailers',
        # 'Cookie': 'PHPSESSID=j58aq4t685in39boic4t7jgkeh',
    }

    data = {
        'draw': '1',
        'columns[0][data]': 'no',
        'columns[0][name]': '',
        'columns[0][searchable]': 'true',
        'columns[0][orderable]': 'true',
        'columns[0][search][value]': '',
        'columns[0][search][regex]': 'false',
        'columns[1][data]': 'header_code',
        'columns[1][name]': '',
        'columns[1][searchable]': 'true',
        'columns[1][orderable]': 'true',
        'columns[1][search][value]': '',
        'columns[1][search][regex]': 'false',
        'columns[2][data]': 'register_number',
        'columns[2][name]': '',
        'columns[2][searchable]': 'true',
        'columns[2][orderable]': 'true',
        'columns[2][search][value]': '',
        'columns[2][search][regex]': 'false',
        'columns[3][data]': 'full_name',
        'columns[3][name]': '',
        'columns[3][searchable]': 'true',
        'columns[3][orderable]': 'true',
        'columns[3][search][value]': '',
        'columns[3][search][regex]': 'false',
        'columns[4][data]': 'request_code',
        'columns[4][name]': '',
        'columns[4][searchable]': 'true',
        'columns[4][orderable]': 'true',
        'columns[4][search][value]': '',
        'columns[4][search][regex]': 'false',
        'columns[5][data]': 'passport_number',
        'columns[5][name]': '',
        'columns[5][searchable]': 'true',
        'columns[5][orderable]': 'true',
        'columns[5][search][value]': '',
        'columns[5][search][regex]': 'false',
        'columns[6][data]': 'paid_date',
        'columns[6][name]': '',
        'columns[6][searchable]': 'true',
        'columns[6][orderable]': 'true',
        'columns[6][search][value]': '',
        'columns[6][search][regex]': 'false',
        'columns[7][data]': 'visa_type',
        'columns[7][name]': '',
        'columns[7][searchable]': 'true',
        'columns[7][orderable]': 'true',
        'columns[7][search][value]': '',
        'columns[7][search][regex]': 'false',
        'columns[8][data]': 'status',
        'columns[8][name]': '',
        'columns[8][searchable]': 'true',
        'columns[8][orderable]': 'true',
        'columns[8][search][value]': '',
        'columns[8][search][regex]': 'false',
        'columns[9][data]': 'actions',
        'columns[9][name]': '',
        'columns[9][searchable]': 'true',
        'columns[9][orderable]': 'true',
        'columns[9][search][value]': '',
        'columns[9][search][regex]': 'false',
        'start': '0',
        'length': '1',
        'search[value]': '',
        'search[regex]': 'false',
    }

    response = requests.post(
        'https://evisa.imigrasi.go.id/web/applications/batch/data',
        cookies=cookies,
        headers=headers,
        data=data,
    )

    if response.status_code == 200:
        return True
    



# Цикл
while True:
    print('Запускаем программу')
    credentials = {
    "type": "service_account",
    "project_id": "visa-center-462016",
    "private_key_id": "158c9eeee827bfa534c1db5bac80de58215e0cca",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCyFZ+wXN8S9RxY\nq6cangLSPKRNtYgAOIhz58Mv861zGS3KLsHg4wc4xDuk+cS00yB12L6suxDVQ7zy\nUR4tT8oYN1rGXVRyoNr4dZShluFva0ndtiKjDfO5EYDZuZRWTTnXg/2ro7qhPNNN\nsHc+kyF//6KQ/fjZnfC4ooBT+vElvOFGdkr61fww4ywORU4XjUEQTqenlRjSWcDT\n8P29k7TRyz+TLi5w4TUAQCuEoN3gtFVDOleM/pPDdpuXLMmuIbnBoO1gEP0F6ZTS\ncVocZsvnKcUfauqD584GeNX1rTk2vldquVa+oshnuJ6amV12EmNg0aZmlKbA/SZf\nBdz/1AT3AgMBAAECggEAVzuFZggQE4KyeHLY345wFlizKQeXj3ghyxjHgUktePhn\n+CZQPR8oTfO9vbHFW8ghmehqekUF2Vmdh9sZW5u+hH80/X8BG0dVC6jY/hBg7EmT\nPMH3lISEku6SfdRMZ7vbbgjPLRD+HfYkEWWz+5hpq//mO3zUtZCKtn6POtPs5Q0f\nuAvpaC+0pmlocEBZ2trocOEb4pyHeJLOVAk2WjOyWHCi8/dVNkyvcCakgZebXa9q\nn6tJR4wr9id9CQIUDGeqTf1uF/pYnEP3AO3I1qg5D3JpynSuPNyP5UZNM36Vt36P\n/NRAj99c40F3ruk2eJrh9QSh2LRhP1a6LsCFwn003QKBgQDeXSWsfR3PBeEqftgB\nwzrkb7HYfvomZO3mlfAhWzPmetsBe8oiVma7gnoForg0BkYsLaNcBydA9FOOzfS0\n1/kDIiNHBeIzFYWo0OetoUDt4Wg5DZMJwfYWsNnSGsAo923CzKXjhriey5cuWo7l\nLpwXFP4HFTZMWtNaNMOjau/MLQKBgQDNBctuGvRz4nGdIfaX8g3sNIfEOU/7gWpA\nG/K4FMxftn4VxpNMapczXoV/cA5+oR2aosxWBbMv6QnRN4NLx9vwB6a7ydFS0NGf\n1bhHQPY43sDQymZ81E+b/cjw4rTyDi6R0sPp6MPLnCrrq/rgcWWedC5AzEPdyNPT\nIYRdb0a4MwKBgFNBmQt+RRzwXKAmogX27SP+1h3zXQHnJFQDq8cxeLtBIKLrkIFO\nzGREtB9MD6AbAUclR1b7rqzZTjfX0Vmsy6VqsL606z6pPkQ5A6W1DLSEgxtpg7ZR\nkyxnxwat0WkFS2l2al5IYPPD0rUeXwZcb0ENMRfBz3TDRQMvYljbfzF5AoGBALWY\nLFeaCGucZoWHT6PGAg71eEuVeAKM3k9qcdIamess+QDQoidNQh992UDHQA9pJY+S\nIusOoWgOQWPOh7zXiTdRj51FZOK1kva/ljmGkJBOvPoyTBTE+L5yS0kRhLPhW95N\nkLneMY5nBJ059zxVNGzk+xLp2jLXbsfTKCqaJUmBAoGBANGz9VeNAzPQLG+Z2PaX\nDDHN3KK31zf5hbTC60UDKKGFhcG9xqn+zCAXWu1zEwk9FTGlfrZrLfbL6qhIEUVA\nhfUx1Ka/0ofSGY1cK3rjXI9N57Q23Row4Bym1c3E63u9iD7pmqYethxHVjY6nBaa\nI4HYguvQQsgV6XbMKwUHbuaC\n-----END PRIVATE KEY-----\n",
    "client_email": "visa-center@visa-center-462016.iam.gserviceaccount.com",
    "client_id": "116963795642618736681",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/visa-center%40visa-center-462016.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
    }


    gc = gspread.service_account_from_dict(credentials)
    spreadsheet = gc.open_by_key("1egoXzhwNY3XD3y8s2zh688OO80fCMJ4QQakN4-dlqCA")  # ID таблицы из URL
    worksheet_account = spreadsheet.worksheet('Аккаунты')
    name_account = worksheet_account.col_values(1)
    password_account = worksheet_account.col_values(2)
    worksheet = spreadsheet.worksheet('Batch Application')
    worksheet.clear()
    worksheet_manager = spreadsheet.worksheet('Batch Application(Manager)')
    worksheet_manager.clear()
    headers = ["Batch No", "Register Number", "Full Name", "Visitor Visa Number", "Passport Number", "Payment Date", "Visa Type", "Status", "Action Link"]
    worksheet.append_row(headers)
    for index, name in enumerate(name_account, 0):
        print(name)
        session_id = load_value(name)
        if not check_session(session_id):
            print('Наш токен входа устарел, обновляем')
            session_id = login(name, password_account[index])

        

        headers = {
            'Host': 'evisa.imigrasi.go.id',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.0',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
            # 'Accept-Encoding': 'gzip, deflate',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            # 'Content-Length': '2222',
            'Origin': 'https://evisa.imigrasi.go.id',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            # Requests doesn't support trailers
            # 'Te': 'trailers',
            # 'Cookie': 'PHPSESSID=18o2dm4tu6jlc3fu2nptrjqqdb',
        }

        client_data = []
        offset = 0

        while True:
            cookies = {
            'PHPSESSID': session_id,
            }

            data = {
                'draw': '1',
                'columns[0][data]': 'no',
                'columns[0][name]': '',
                'columns[0][searchable]': 'true',
                'columns[0][orderable]': 'true',
                'columns[0][search][value]': '',
                'columns[0][search][regex]': 'false',
                'columns[1][data]': 'header_code',
                'columns[1][name]': '',
                'columns[1][searchable]': 'true',
                'columns[1][orderable]': 'true',
                'columns[1][search][value]': '',
                'columns[1][search][regex]': 'false',
                'columns[2][data]': 'register_number',
                'columns[2][name]': '',
                'columns[2][searchable]': 'true',
                'columns[2][orderable]': 'true',
                'columns[2][search][value]': '',
                'columns[2][search][regex]': 'false',
                'columns[3][data]': 'full_name',
                'columns[3][name]': '',
                'columns[3][searchable]': 'true',
                'columns[3][orderable]': 'true',
                'columns[3][search][value]': '',
                'columns[3][search][regex]': 'false',
                'columns[4][data]': 'request_code',
                'columns[4][name]': '',
                'columns[4][searchable]': 'true',
                'columns[4][orderable]': 'true',
                'columns[4][search][value]': '',
                'columns[4][search][regex]': 'false',
                'columns[5][data]': 'passport_number',
                'columns[5][name]': '',
                'columns[5][searchable]': 'true',
                'columns[5][orderable]': 'true',
                'columns[5][search][value]': '',
                'columns[5][search][regex]': 'false',
                'columns[6][data]': 'paid_date',
                'columns[6][name]': '',
                'columns[6][searchable]': 'true',
                'columns[6][orderable]': 'true',
                'columns[6][search][value]': '',
                'columns[6][search][regex]': 'false',
                'columns[7][data]': 'visa_type',
                'columns[7][name]': '',
                'columns[7][searchable]': 'true',
                'columns[7][orderable]': 'true',
                'columns[7][search][value]': '',
                'columns[7][search][regex]': 'false',
                'columns[8][data]': 'status',
                'columns[8][name]': '',
                'columns[8][searchable]': 'true',
                'columns[8][orderable]': 'true',
                'columns[8][search][value]': '',
                'columns[8][search][regex]': 'false',
                'columns[9][data]': 'actions',
                'columns[9][name]': '',
                'columns[9][searchable]': 'true',
                'columns[9][orderable]': 'true',
                'columns[9][search][value]': '',
                'columns[9][search][regex]': 'false',
                'start': offset,
                'length': '100000',
                'search[value]': '',
                'search[regex]': 'false',
            }

            response = requests.post(
                'https://evisa.imigrasi.go.id/web/applications/batch/data',
                headers=headers,
                data=data,
                cookies=cookies
            )
            print('Запрос на получении Batch Application', response)
            result = response.json().get('data', [])
            for res in result:
                batch_no = res.get('header_code', '').strip().replace('\n', '')
                register_number = res.get('register_number', '')
                full_name = res.get('full_name', '')
                visitor_visa_number = res.get('request_code', '')
                passport_number = res.get('passport_number')
                payment_date = res.get('paid_date', '')
                visa_type = res.get('visa_type', '')
                status = res.get('status', '').split('badge bg-success">')[-1].split('</span>')[0].split('">')[-1]
                action = res.get('actions', '').split('href="')[-1].split('" ')[0]
                if not action.split('/')[-1] != 'print':
                    action_link = ''
                else:
                    action_link = f"https://evisa.imigrasi.go.id{action}"
                client_data.append([batch_no,
                                    register_number,
                                    full_name,
                                    visitor_visa_number,
                                    passport_number,
                                    payment_date,
                                    visa_type,
                                    status,
                                    action_link])

            offset += 850
            if not result:
                break
        print(f'Заполняем гугл таблицу Batch Application по данным из аккаунта {name} ')
        worksheet.append_rows(client_data)

    spreadsheet = gc.open_by_key("1tzSQbkOYFAzv8T0d_pLG8ZlHo27Le6CyC2GDJbLFTm4")  # ID таблицы из URL
    worksheet_account = spreadsheet.worksheet('Аккаунты')
    name_account = worksheet_account.col_values(1)
    password_account = worksheet_account.col_values(2)
    worksheet = spreadsheet.worksheet('StayPermit')
    worksheet.clear()
    headers_new = ["Name", "Type of Stypermit", "Visa type",  "Arrival date", "Issue date", "Expired data", "Status", "Account"]
    worksheet.append_row(headers_new)

    for index, name in enumerate(name_account, 0):
        print(name)
        session_id = load_value(name)
        if not check_session(session_id):
            print('Наш токен входа устарел, обновляем')
            session_id = login(name, password_account[index])
        
        offset = 0
        client_data = []
        while True:

            cookies = {
    'PHPSESSID': session_id,
            }

            headers = {
                'Host': 'evisa.imigrasi.go.id',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:139.0) Gecko/20100101 Firefox/139.0',
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
                # 'Accept-Encoding': 'gzip, deflate',
                'X-Requested-With': 'XMLHttpRequest',
                'Referer': 'https://evisa.imigrasi.go.id/front/applications/stay-permit?token=44ea17f.BokM81ow0IGl896FsTeMnVvKN3OhM-In8KgbiFUDrpo.YOJ7lgNYg_PKvLrzwW_nzWODQzDESoZEk9534yVq58xCwz28OXu2zvWLrQ',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                # Requests doesn't support trailers
                # 'Te': 'trailers',
                # 'Cookie': 'PHPSESSID=cjq3epk1vi4og44kg3sqc4egtn',
            }

            params = {
                'draw': '1',
                'columns[0][data]': 'register_number',
                'columns[0][name]': '',
                'columns[0][searchable]': 'true',
                'columns[0][orderable]': 'true',
                'columns[0][search][value]': '',
                'columns[0][search][regex]': 'false',
                'columns[1][data]': 'full_name',
                'columns[1][name]': '',
                'columns[1][searchable]': 'true',
                'columns[1][orderable]': 'true',
                'columns[1][search][value]': '',
                'columns[1][search][regex]': 'false',
                'columns[2][data]': 'permit_number',
                'columns[2][name]': '',
                'columns[2][searchable]': 'true',
                'columns[2][orderable]': 'true',
                'columns[2][search][value]': '',
                'columns[2][search][regex]': 'false',
                'columns[3][data]': 'type_of_staypermit',
                'columns[3][name]': '',
                'columns[3][searchable]': 'true',
                'columns[3][orderable]': 'true',
                'columns[3][search][value]': '',
                'columns[3][search][regex]': 'false',
                'columns[4][data]': 'visa_number',
                'columns[4][name]': '',
                'columns[4][searchable]': 'true',
                'columns[4][orderable]': 'true',
                'columns[4][search][value]': '',
                'columns[4][search][regex]': 'false',
                'columns[5][data]': 'type_of_visa',
                'columns[5][name]': '',
                'columns[5][searchable]': 'true',
                'columns[5][orderable]': 'true',
                'columns[5][search][value]': '',
                'columns[5][search][regex]': 'false',
                'columns[6][data]': 'passport_number',
                'columns[6][name]': '',
                'columns[6][searchable]': 'true',
                'columns[6][orderable]': 'true',
                'columns[6][search][value]': '',
                'columns[6][search][regex]': 'false',
                'columns[7][data]': 'start_date',
                'columns[7][name]': '',
                'columns[7][searchable]': 'true',
                'columns[7][orderable]': 'true',
                'columns[7][search][value]': '',
                'columns[7][search][regex]': 'false',
                'columns[8][data]': 'issue_date',
                'columns[8][name]': '',
                'columns[8][searchable]': 'true',
                'columns[8][orderable]': 'true',
                'columns[8][search][value]': '',
                'columns[8][search][regex]': 'false',
                'columns[9][data]': 'expired_date',
                'columns[9][name]': '',
                'columns[9][searchable]': 'true',
                'columns[9][orderable]': 'true',
                'columns[9][search][value]': '',
                'columns[9][search][regex]': 'false',
                'columns[10][data]': 'status',
                'columns[10][name]': '',
                'columns[10][searchable]': 'true',
                'columns[10][orderable]': 'true',
                'columns[10][search][value]': '',
                'columns[10][search][regex]': 'false',
                'columns[11][data]': 'action',
                'columns[11][name]': '',
                'columns[11][searchable]': 'true',
                'columns[11][orderable]': 'true',
                'columns[11][search][value]': '',
                'columns[11][search][regex]': 'false',
                'start': offset,
                'length': '100000000',
                'search[value]': '',
                'search[regex]': 'false',
                '_': '1749185171731',
            }

            response = requests.get(
                'https://evisa.imigrasi.go.id/front/applications/stay-permit/data',
                params=params,
                cookies=cookies,
                headers=headers,
                verify=False,
            )
            print(f'StayPermit {response}')
            result = response.json().get('data', [])
            for res in result:
                try:
                    reg_number = extract_reg_number(safe_get(res, 'register_number'))
                    full_name = safe_get(res, 'full_name')
                    permit_number = safe_get(res, 'permit_number')
                    type_permit = safe_get(res, 'type_of_staypermit')
                    visa_number = safe_get(res, 'visa_number')
                    type_visa = safe_get(res, 'type_of_visa')
                    passport_number = safe_get(res, 'passport_number')
                    start_date = safe_get(res, 'start_date')
                    issue_data = safe_get(res, 'issue_date')
                    expired_data = safe_get(res, 'expired_date')

                    # Извлечение статуса
                    status = extract_status(safe_get(res, 'status'))

                    client_data.append([
                        full_name,
                        type_permit,
                        type_visa,
                        start_date,
                        issue_data,
                        expired_data,
                        status,
                        name
                    ])

                    #Debugging
                    #print(client_data,end='\n')

                except Exception as e:
                    print(f"Error processing record: {res}. Error: {e}")
            offset += 1250
            if not result:
                break

        print(f'Заполняем гугл таблицу StayPermit по данным из аккаунта {name} ')
        #print(client_data)
        worksheet.append_rows(client_data)

        print(f'Заполнили гугл таблицу по данным из аккаунта {name} ')

    time.sleep(10000)