import logging
import os

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(module)s] %(message)s',
    handlers=[
        logging.StreamHandler(),  
        logging.FileHandler("logs/app.log", encoding='utf-8')  
    ]
)

logger = logging.getLogger(__name__)

logging.getLogger("google.auth.transport").setLevel(logging.WARNING)
logging.getLogger("google.auth.client").setLevel(logging.WARNING)
logging.getLogger("oauth2client").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("yadisk").setLevel(logging.WARNING)