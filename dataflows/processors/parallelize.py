import itertools
from re import match
from build.lib.dataflows.helpers.resource_matcher import ResourceMatcher
from build.lib.dataflows.base.package_wrapper import PackageWrapper
from build.lib.dataflows.base.resource_wrapper import ResourceWrapper
import os
import multiprocessing as mp
import threading
import queue

from .. import Flow
from ..helpers import ResourceMatcher

def init_mp(num_processors, row_func, q_in, q_internal):
    print('init_mp')
    q_out = mp.Queue()
    processes = [mp.Process(target=work, args=(q_in, q_out, row_func)) for _ in range(num_processors)]
    for process in processes:
        process.start()
    t_fetch = threading.Thread(target=fetcher, args=(q_out, q_internal, num_processors))
    t_fetch.start()
    print('init_mp done')
    return (processes, t_fetch)


def fini_mp(processes, t_fetch):
    print('fini_mp')
    for i, process in enumerate(processes):
        try:
            print('joining', i)
            process.join(timeout=10)
        except Exception as e:
            try:
                print('killing', i)
                process.kill()
            except:
                pass
        finally:
            process.close()
    print('joining t_fetch')
    t_fetch.join()
    print('fini_mp done')


def producer(res, q_in, q_internal, num_processors, predicate):
    print('producer starting')
    try:
        for row in res:
            if predicate(row):
                q_in.put(row)
            else:
                q_internal.put(row)
        for _ in range(num_processors):
            q_in.put(None)
        print('producer finished iteration')
    except Exception as e:
        print('FAILED TO WRITE TO QUEUE', e)
        q_internal.put(None)
        return 1
    print('producer exiting')    
    return 0


def fetcher(q_out, q_internal, num_processors):
    expected_nones = num_processors
    print('fetcher starting')    
    while True:
        row = q_out.get()
        if row is None:
            expected_nones -= 1
            print('fetcher expected_nones', expected_nones)                
            if expected_nones == 0:
                q_internal.put(None)
                break
            continue
        q_internal.put(row)
    print('fetcher done')    


def work(q_in: mp.Queue, q_out: mp.Queue, row_func):
    count = 0
    pid = os.getpid()
    print(pid, 'worker starting')
    try:
        while True:
            row = q_in.get()
            if count % 100 == 0:
                print(pid, 'worker got', count, 'rows')
            if row is None:
                print(pid, 'worker got stop indication')
                break
            count += 1
            try:
                row_func(row)
            except Exception as e:
                print(pid, 'FAILED TO RUN row_func {}\n'.format(e))
                pass
            q_out.put(row)
    except Exception as e:
        print(pid, 'worker failed', e)
        pass
    finally:
        q_out.put(None)
        print(pid, 'worker ending')


def fork(res, row_func, num_processors, predicate):
    predicate = predicate or (lambda x: True)
    for row in res:
        if predicate(row):
                print('fork starting')
                res = itertools.chain([row], res)
                q_in = mp.Queue()
                q_internal = queue.Queue()
                t_prod = threading.Thread(target=producer, args=(res, q_in, q_internal, num_processors, predicate))
                t_prod.start()

                processes, t_fetch = init_mp(num_processors, row_func, q_in, q_internal)

                print('fork waiting for data')  
                count = 0              
                while True:
                    row = q_internal.get()
                    if row is None:
                        break
                    count += 1
                    if count % 100 == 0:
                        print('fork got %d rows' % count)
                    yield row
                print('fork done waiting for data')                
                t_prod.join()
                fini_mp(processes, t_fetch)
                print('fork finished')
        else:
            yield row


def parallelize(row_func, num_processors=None, resources=None, predicate=None):
    num_processors = num_processors or 2*os.cpu_count()

    def func(package: PackageWrapper):
        yield package.pkg
        matcher = ResourceMatcher(resources, package.pkg)
        
        res: ResourceWrapper
        for res in package:
            if matcher.match(res.res.name):
                yield fork(res, row_func, num_processors, predicate)
            else:
                yield res
    
    return func
