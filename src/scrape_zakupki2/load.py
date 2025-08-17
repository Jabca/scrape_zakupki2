from dataclasses import dataclass
from datetime import date
from io import StringIO
from pathlib import Path
import urllib.parse as urlp

import bs4
from loguru import logger
import pandas as pd

from scrape_zakupki2.utils import RequestSessioner

HOME_PAGE_BASEURL = (
    (Path(__file__).parent / "links" / "home_page.txt").resolve().read_text()
)
DOWNLOAD_BASEURL = (
    (Path(__file__).parent / "links" / "load_link.txt").resolve().read_text()
)


@dataclass
class load_arguments:
    search_str: str
    pubdate_from: date
    pubdate_to: date
    include_fz44: bool
    include_fz223: bool
    request_tries = 10
    request_delay = 0.5
    to: int | None = None
    from_: int | None = None

    @property
    def pubdate_from_s(self) -> str:
        return f"{self.pubdate_from.day:02d}.{self.pubdate_from.month:02d}.{self.pubdate_from.year}"

    @property
    def pubdate_to_s(self) -> str:
        return f"{self.pubdate_to.day:02d}.{self.pubdate_to.month:02d}.{self.pubdate_to.year}"


def _modify_query(base_url: str, query: dict[str, str]) -> str:
    parts = urlp.urlparse(base_url)
    _query = urlp.parse_qs(parts.query)
    for key, val in query.items():
        _query[key] = [val]
    new_url = urlp.urlunparse(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            parts.params,
            urlp.urlencode(_query, doseq=True),
            parts.fragment,
        )
    )

    return new_url


def load_num_results(
    args: load_arguments,
) -> tuple[int | None, tuple[date, date] | None]:
    url = _modify_query(
        HOME_PAGE_BASEURL,
        {
            "searchString": args.search_str,
            "fz44": "on" if args.include_fz44 else "off",
            "fz223": "on" if args.include_fz44 else "off",
            "publishDateFrom": args.pubdate_from_s,
            "publishDateTo": args.pubdate_to_s,
        },
    )

    try:
        getter = RequestSessioner(tries=args.request_tries, delay=args.request_delay)
        html = getter.get_text(url=url)
    except Exception as e:
        logger.error(f"Wasn't able to load results for {args} cause of {e}")
        return None, None
    finally:
        if "getter" in locals():
            getter.close()  # type: ignore

    try:
        soup = bs4.BeautifulSoup(html, "html.parser")
        count = soup.find("div", class_="search-results__total").text  # type: ignore
        assert isinstance(count, str)
        number = "".join(list(filter(lambda c: c.isdigit(), count)))
        return int(number), (args.pubdate_from, args.pubdate_to)
    except Exception as e:
        logger.error(f"Couldn't get number of results for {args} because of {e}")
        return None, None


def load_csv(args: load_arguments, pool_size: int = 4) -> pd.DataFrame | None:
    assert args.to is not None and args.from_ is not None
    url = _modify_query(
        DOWNLOAD_BASEURL,
        {
            "searchString": args.search_str,
            "fz44": "on" if args.include_fz44 else "off",
            "fz223": "on" if args.include_fz44 else "off",
            "publishDateFrom": args.pubdate_from_s,
            "publishDateTo": args.pubdate_to_s,
            "from": str(args.from_),
            "to": str(args.to),
        },
    )

    try:
        getter = RequestSessioner(tries=args.request_tries, delay=args.request_delay)
        res = getter.get_content(url)
        logger.trace(res)

        return pd.read_csv(StringIO(res.decode("cp1251")), sep=";", decimal=",")  # type: ignore

    except Exception as e:
        logger.error(f"Couldn't process {url} because of {e}")
        return None
    finally:
        if "getter" in locals():
            getter.close()  # type: ignore
