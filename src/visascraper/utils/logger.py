from __future__ import annotations

import logging

from visascraper.config import ensure_runtime_dirs, settings

ensure_runtime_dirs()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(settings.logs_dir / "app.log", encoding="utf-8"),
    ],
)

logger = logging.getLogger("visascraper")

logging.getLogger("google.auth.transport").setLevel(logging.WARNING)
logging.getLogger("google.auth.client").setLevel(logging.WARNING)
logging.getLogger("oauth2client").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("yadisk").setLevel(logging.WARNING)

logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("apscheduler.executors.default").setLevel(logging.WARNING)
logging.getLogger("apscheduler.scheduler").setLevel(logging.WARNING)
