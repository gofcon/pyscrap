"""Which engine runs an ``ApiMst`` row.

``request_type`` decides, and it is the only thing that does: ``'BROWSER'``
means the row is acted out in a real browser (:mod:`app.scrapers.browser`),
anything else is an HTTP request (:mod:`app.scrapers.dynamic`). Both hand back
the same ``{selector: [record, ...]}`` and save it through the same
:class:`app.scrapers.base.BaseScraper`, so everything downstream -- job
generation, logging, result tables, export -- is unaware there are two.

Both engines are imported lazily, on the row that needs one. That keeps the
API deployment (which generates and validates jobs, see
:mod:`app.api.routers.jobs`) from needing playwright and a chromium install to
answer a request it will never use them for -- browser jobs run on the
instance, where the ``[browser]`` extra is installed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from app.db.models import ApiMst
    from app.scrapers.base import BaseScraper

BROWSER_REQUEST_TYPE = "BROWSER"


def make_scraper(api: ApiMst, params: dict[str, Any] | None = None) -> BaseScraper:
    """The engine this row asks for, ready to run."""
    if (api.request_type or "").upper() == BROWSER_REQUEST_TYPE:
        from app.scrapers.browser import BrowserScraper

        return BrowserScraper(api, params=params)

    from app.scrapers.dynamic import DynamicApiScraper

    return DynamicApiScraper(api, params=params)
