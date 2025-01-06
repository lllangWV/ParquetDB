from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathos.threading import ThreadPool

# def parallel_apply(func, data, executor=None):
    
#     if executor is None:
#         executor=ThreadPoolExecutor()
#     with executor:
#         future = executor.submit(func, data)
#         return future.result()
    

def parallel_apply(func, data, executor=None):
    
    if executor is None:
        executor=ThreadPool()
    with executor:
        future = executor.map(func, data)
        return future

