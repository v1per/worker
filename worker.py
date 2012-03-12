from threading import Thread, Lock
import logging

log = logging.getLogger()

class WorkerPool(object): 
    def __init__(self, task, in_data, count=1):
        self.task = task
        self.in_data_next =  iter(in_data).next
        self.out_data = {}
        self.lock = Lock()
        self.pool = [Thread(target=self._do_task) for i in range(count)]

    def work(self):
        for thread in self.pool:
            thread.start()

        for thread in self.pool:
            thread.join()

        return self.out_data

    def _do_task(self):
        while True:
            self.lock.acquire()
            try:
               in_data = self.in_data_next()
            except StopIteration:
                break
            finally:
                self.lock.release()
            
            log.info('Starting executing task for data: %s' % (in_data, )) 
            out_data = self.task(in_data)
            log.info('Finishing executing task for data: %s' % (in_data, ))

            self.lock.acquire()
            self.out_data[in_data] = out_data
            self.lock.release()
            log.info('Collecting out_data for in_data %s' % (in_data, ))
        
        log.info('Worker finished his task')


if __name__ == '__main__':
    import time
    
    def wait(data):
        print "Waiting task: %s" % data
        time.sleep(30)

    start = time.time()
    worker_pool = WorkerPool(wait, range(10), 5)
    worker_pool.work()
    end = time.time()

    print "Execution time by 5 workers: %dm and %ds" % ((end - start) / 60,
                                                        (end - start) % 60)

    start = time.time()
    for i in range(10):
        wait(i)
    end = time.time()

    print "Execution time by loop: %dm and %ds" % ((end - start) / 60, 
                                                   (end - start) % 60)



