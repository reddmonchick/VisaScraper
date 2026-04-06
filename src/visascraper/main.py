from __future__ import annotations

import asyncio

from visascraper.app import Application


def main() -> None:
    app = Application()
    asyncio.run(app.run())


if __name__ == "__main__":
    main()
