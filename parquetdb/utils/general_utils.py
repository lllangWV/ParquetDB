import copy
import os
import logging
import functools
import random
import time

logger = logging.getLogger(__name__)
time_logger = logging.getLogger('timing')

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
        time_logger.debug(f"Function {func.__name__!r} executed in {elapsed_time:.4f} seconds")
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



def generate_similar_data(template_data, num_entries):
    def generate_value(value):
        if isinstance(value, int):
            return random.randint(value - 10, value + 10)
        elif isinstance(value, float):
            return round(random.uniform(value * 0.8, value * 1.2), 2)
        elif isinstance(value, str):
            return f"{value}_{random.randint(1, 100)}"
        elif isinstance(value, dict):
            return {k: generate_value(v) for k, v in value.items()}
        # elif isinstance(value, list):
        #     return [generate_value(item) for item in value]
        elif value is None:
            return None
        else:
            return value

    generated_data = []
    for i in range(num_entries):
        new_entry = copy.deepcopy(random.choice(template_data))
        for key, value in new_entry.items():
            if key == 'id':
                new_entry[key] = i
            else:
                new_entry[key] = generate_value(value)
        generated_data.append(new_entry)

    return generated_data