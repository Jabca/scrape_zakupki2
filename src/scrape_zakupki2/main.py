from datetime import date, timedelta
from pathlib import Path
import tempfile
from typing import Any
import dask
from dask.delayed import delayed, Delayed, DelayedLeaf
from dask.distributed import Client
import dask.distributed
import pandas as pd
import dask.dataframe as dd
from dask.distributed import as_completed

from scrape_zakupki2.load import load_arguments, load_csv, load_num_results


def scrape_zakupki(
    dask_client: Client,
    search_str: str = "болт",
    pubdate_to: date = date.today(),
    pubdate_from: date = date.today() - timedelta(days=365),
    timedelta_step: timedelta = timedelta(days=30),
    include_fz44: bool = True,
    include_fz223: bool = True,
    memory_limit: str = "2GB",
) -> dd.DataFrame | None:
    """
    Function to get contracts data from goszakupki site. Uses dask Client and closes it after usage

    Args:
        search_str (str):
            Search string for which to search on goszakupki.
            Default: "болт"
        pubdate_from (date):
            Start date of contract publication period (inclusive).
            Default: 365 days before today
        pubdate_to (date):
            End date of contract publication period (inclusive).
            Default: today
        timedelta_step (timedelta):
            Time interval for splitting the search into smaller chunks.
            Smaller step -> more API calls but potentially more reliable results.
            Default: 30 days
        include_fz44 (bool):
            Include contracts under Federal Law 44 (ФЗ-44).
            Default: True
        include_fz223 (bool):
            Include contracts under Federal Law 223 (ФЗ-223).
            Default: True
        pool_size (int):
            Number of concurrent threads for scraping.
            Default: 4

    Returns:
        out (dd.DataFrame | None):
            Pandas DataFrame with scraped contract data if search was successful,
            None if no data was found or scraped.

    Raises:
        ValueError: If input parameters are invalid (e.g., pubdate_from > pubdate_to)
    """

    if (pubdate_to - pubdate_from).days < 0:
        raise ValueError(
            f"Date's range is invalid from: {pubdate_from} is greater then to {pubdate_from}"
        )

    if timedelta_step.days < 0:
        raise ValueError(
            f"step is negative: {timedelta_step} so the search will never finish"
        )

    # divide search date range into small date ranges
    left = pubdate_from
    right = left + timedelta_step
    right = min(right, pubdate_to)

    futures_0: list[Any] = list()

    while right <= pubdate_to and left < pubdate_to:
        args = load_arguments(
            search_str=search_str,
            pubdate_from=left,
            pubdate_to=right,
            include_fz44=include_fz44,
            include_fz223=include_fz223,
        )

        futures_0.append(dask_client.compute(load_num_results(args)))

        left += timedelta_step
        right += timedelta_step
        right = min(right, pubdate_to)

    tasks: list[load_arguments] = list()
    # for each date range create ranges with 500 in each (cause stuff works like this)
    for future in as_completed(futures_0):
        num, dates = future.result()
        if num < 0:
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

    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file_storage = Path(temp_dir)

            @delayed
            def process_and_save(args: load_arguments, chunk_id: int) -> Path | None:
                df_chunk = load_csv(args)
                if df_chunk is None:
                    return None
                chunk_path = temp_file_storage / f"chunk#{chunk_id}.parquet"
                df_chunk.to_parquet(chunk_path, index=False)

            delayed_paths = [process_and_save(args, i) for i, args in enumerate(tasks)]

            paths: list[Path | None] = dask_client.compute(delayed_paths, sync=True)  # type: ignore

            valid_paths = [p for p in paths if p is not None]

            if len(valid_paths) == 0:
                return None

            return dd.read_parquet([str(p) for p in valid_paths])  # type: ignore

    finally:
        dask_client.close()


if __name__ == "__main__":
    cluster = dask.distributed.LocalCluster(
        n_workers=2, memory_limit="4GB", processes=True, silence_logs=50
    )
    res = scrape_zakupki(
        cluster.get_client(),
        search_str="болт",
        pubdate_from=date.today() - timedelta(days=10),
        timedelta_step=timedelta(days=1),
    )

    if res is not None:
        res.to_csv("test.csv", single_file=True)
    print(res)
