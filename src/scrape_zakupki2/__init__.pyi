from datetime import date, timedelta
import tempfile
import pandas as pd

def scrape_zakupki(
    search_str: str = "болт",
    pubdate_to: date = date.today(),
    pubdate_from: date = date.today() - timedelta(days=365),
    timedelta_step: timedelta = timedelta(days=30),
    include_fz44: bool = True,
    include_fz223: bool = True,
    pool_size: int = 4,
) -> pd.DataFrame | None: ...
def scrape_zakupki_low_ram(
    search_str: str = "болт",
    pubdate_to: date = date.today(),
    pubdate_from: date = date.today() - timedelta(days=365),
    timedelta_step: timedelta = timedelta(days=30),
    include_fz44: bool = True,
    include_fz223: bool = True,
    pool_size: int = 4,
) -> list[tempfile._TemporaryFileWrapper[bytes]] | None:
    """
    Equivallent to scrape_zakupki, but returns list of TextIOWrappers which which point to csv files were data was loaded.
    """
    ...
