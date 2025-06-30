from python_rucaptcha.re_captcha import ReCaptcha

RUCAPTCHA_KEY = 'bd9c67bafe49a8410846e953fd04ff49'

def solve_recaptcha(site_key: str, page_url: str) -> str | None:
    """Решает reCAPTCHA и возвращает токен."""
    result = ReCaptcha(rucaptcha_key=RUCAPTCHA_KEY,
                       websiteKey=site_key,
                       websiteURL=page_url).captcha_handler()
    
    if result.get('solution') is None:
        print("Ошибка решения капчи:", result.get('errorDescription'))
        return None
    return result['solution'].get('gRecaptchaResponse')