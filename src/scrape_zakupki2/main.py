from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Any
import pandas as pd

from scrape_zakupki2.load import load_arguments, load_csv, load_num_results


def scrape_zakupki(
    search_str: str = "болт",
    pubdate_to: date = date.today(),
    pubdate_from: date = date.today() - timedelta(days=365),
    timedelta_step: timedelta = timedelta(days=30),
    include_fz44: bool = True,
    include_fz223: bool = True,
    pool_size: int = 4,
) -> pd.DataFrame | None:
    """
    This function will load results for searchString into 1 dataframe. Will raise ValueError if the arguments are invalid. will return None if no data was collected
    """

    if (pubdate_to - pubdate_from).days < 0:
        raise ValueError(
            f"Date's range is invalid from: {pubdate_from} is greater then to {pubdate_from}"
        )

    if timedelta_step.days < 0:
        raise ValueError(
            f"step is negative: {timedelta_step} so the search will never finish"
        )

    futures: list[Any] = list()

    # divide search date range into small
    with ThreadPoolExecutor(max_workers=pool_size) as exe:
        left = pubdate_from
        right = left + timedelta_step
        right = min(right, pubdate_to)

        while right <= pubdate_to and left < pubdate_to:
            args = load_arguments(
                search_str=search_str,
                pubdate_from=left,
                pubdate_to=right,
                include_fz44=include_fz44,
                include_fz223=include_fz223,
            )

            futures.append(exe.submit(load_num_results, args))

            left += timedelta_step
            right += timedelta_step
            right = min(right, pubdate_to)

    tasks: list[load_arguments] = list()
    # for each date range create ranges with 500 in each (cause stuff works like this)
    for future in as_completed(futures):
        num, dates = future.result()
        if num is None:
            continue
        left = 1
        right = min(500, num)
        while right <= num and left < num:
            tasks.append(
                load_arguments(
                    search_str=search_str,
                    pubdate_from=dates[0],
                    pubdate_to=dates[1],
                    include_fz223=include_fz223,
                    include_fz44=include_fz44,
                    from_=left,
                    to=right,
                )
            )
            left += 500
            right += 500

    futures.clear()
    with ThreadPoolExecutor(max_workers=pool_size) as exec:
        for task in tasks:
            futures.append(exec.submit(load_csv, task))

    results: list[pd.DataFrame] = list()
    for future in as_completed(futures):
        res = future.result()
        if res is None:
            continue
        results.append(res)

    return pd.concat(results)


if __name__ == "__main__":
    res = scrape_zakupki(search_str="бумага", pubdate_from=date(2024, 3, 1))

    with open("test.csv", mode="w") as f:
        if res is not None:
            res.to_csv(f, sep=";", decimal=",")
    print(res)
