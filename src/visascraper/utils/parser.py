from bs4 import BeautifulSoup

def safe_get(data: dict, key: str, default='') -> str:
    """Безопасно извлекает значение из словаря. Возвращает пустую строку, если ключ отсутствует или значение None."""
    return data.get(key, default)

def extract_status_batch(html_content: str) -> str:
    """Извлекает текст статуса из HTML-контента (для Batch Application)"""
    if not html_content:
        return ''
    try:
        soup = BeautifulSoup(html_content, 'lxml')
        status_span = soup.find('span')
        return status_span.text.strip() if status_span else ''
    except Exception as e:
        print(f"Error extracting batch status: {e}")
        return ''

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
    #base_url = "https://evisa.imigrasi.go.id" 
    if not html_content:
        return ''
    try:
        soup = BeautifulSoup(html_content, 'lxml')
        action_link = soup.find('a', class_='btn btn-sm btn-outline-info')
        if action_link and action_link.has_attr('href'):
            #return f"{base_url}{action_link['href']}"
            return action_link['href']
        return ''
    except Exception as e:
        print(f"Error extracting action link: {e}")
        return ''

def extract_reg_number(html_content: str) -> str:
    """Извлекает номер регистрации"""
    if not html_content:
        return ''
    try:
        reg_number = BeautifulSoup(html_content, 'lxml')
        number = reg_number.find('a').text
        return number
    except:
        return html_content

def extract_visa(html_content: str) -> str:
    """Извлекает ссылку на пдф визы"""
    if not html_content:
        return ''
    try:
        action_link = BeautifulSoup(html_content, 'lxml')
        link = action_link.find('a', class_='fw-bold btn btn-sm btn-outline-info btn-back')['href']
        return link
    except:
        return ''
    
def extract_detail(html_content: str) -> str:
    if not html_content:
        return ''
    try:
        detail_content = BeautifulSoup(html_content, 'lxml')
        detail_link = detail_content.find('a', class_='btn btn-sm btn-primary')['href']
        return detail_link
    except Exception as exc:
        return ''