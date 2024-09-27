import os
import logging
import functools
import time

logger = logging.getLogger(__name__)

import pyarrow as pa

def timeit(func):
    """
    A decorator that measures the execution time of a function.

    Args:
        func: The function to be timed.

    Returns:
        The wrapped function.

    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.debug(f"Function {func.__name__!r} executed in {elapsed_time:.4f} seconds")
        return result

    return wrapper


def is_directory_empty(directory_path:str):
    """
    Checks if a directory is empty.

    Args:
        directory_path (str): The path to the directory.

    Returns:
        bool: True if the directory is empty, False otherwise.
    """

    return not os.listdir(directory_path)


def is_contained(list1, list2):
    """
    Checks if a list is contained in another list.

    Args:
        list1 (list): The first list.
        list2 (list): The second list.

    Returns:
        bool: True if list1 is contained in list2, False otherwise.
    """
    return set(list1).issubset(set(list2))



