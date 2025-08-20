from datetime import date, timedelta
import pandas as pd

from typing import type_check_only

@type_check_only
def scrape_zakupki(
    search_str: str = "болт",
    pubdate_to: date = date.today(),
    pubdate_from: date = date.today() - timedelta(days=365),
    timedelta_step: timedelta = timedelta(days=30),
    include_fz44: bool = True,
    include_fz223: bool = True,
    pool_size: int = 4,
) -> pd.DataFrame | None: ...
