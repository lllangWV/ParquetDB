import logging
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from functools import partial

from dask.distributed import Client
from pathos.pools import ProcessPool
from pathos.threading import ThreadPool

from parquetdb.utils.config import config

logger = logging.getLogger(__name__)

# def parallel_apply(func, data, executor=None):

#     if executor is None:
#         executor=ThreadPoolExecutor()
#     with executor:
#         future = executor.submit(func, data)
#         return future.result()


# def parallel_apply(func, data, executor=None):

#     if executor is None:
#         executor = ProcessPool()

#     if len(data) > 3500:
#         with executor:
#             future = executor.map(func, data)
#             return future
#     else:
#         results = [func(item) for item in data]
#         return results


def parallel_apply(func, data, processes=False):
    if len(data) > 2000 and config.use_multiprocessing:
        with Client(
            silence_logs=logging.ERROR,
            processes=processes,
        ) as client:
            serialized_futures = client.map(func, data)
            results = client.gather(serialized_futures)
    else:
        results = [func(item) for item in data]
    return results


def mp_task(func, list, n_cores=None, **kwargs):
    """
    Processes tasks in parallel using a pool of worker processes.

    This function applies a given function to a list of items in parallel, using
    multiprocessing with a specified number of cores. Each item in the list is processed
    by the function, and additional arguments can be passed through `kwargs`.
    By defualt, it will detect the number of cores available unless specified otherwise.

    Parameters
    ----------
    func : Callable
        The function to be applied to each item in the list.
    list : list
        A list of items to be processed by the function.
    n_cores : int, optional
        The number of cores to use for multiprocessing (default is None).
    **kwargs
        Additional keyword arguments to be passed to `func`.

    Returns
    -------
    list
        A list of results obtained by applying `func` to each item in the input list.
    """
    if n_cores is None:
        executor = ProcessPool()
    else:
        executor = ProcessPool(n_cores)
    try:
        if len(list) > 3500:
            with executor:
                results = executor.map(partial(func, **kwargs), list)
        else:
            results = [func(item, **kwargs) for item in list]
        logger.info("Tasks processed successfully.")

        return results
    except:
        logging.exception("Error processing tasks in parallel.")
