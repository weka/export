
"""
async.py - subprocesses to execute multithreaded API calls.
"""
import pickle
import multiprocessing
from logging import debug, info, warning, error, critical, getLogger, DEBUG
import threading
import wekalib
import time

# initialize logger - configured in main routine
log = getLogger(__name__)

# this is a hidden class
class Job(object):
    def __init__(self, hostname, category, stat, method, parms):
        self.hostname = hostname
        self.category = category
        self.stat = stat
        self.method = method
        self.parms = parms
        self.result = dict()
        self.exception = False


# this is a hidden class
class SlaveThread(object):
    def __init__(self, cluster, outputq):
        self.cluster = cluster
        self.outputq = outputq
        self.inputq = multiprocessing.JoinableQueue(1000) # 100,000 max entries?

        self.thread = threading.Thread(target=self.slave_thread, daemon=True)
        self.thread.start()

    def slave_thread(self):
        while True:
            try:
                job = self.inputq.get()
            except EOFError:
                return  # just silently die - this happens when the parent exits

            hostobj = self.cluster.get_hostobj_byname(job.hostname)
            try:
                job.result = hostobj.call_api(job.method, job.parms)
                job.exception = False
            except wekalib.exceptions.HTTPError as exc:
                if exc.code == 502:  # Bad Gateway
                    log.error(f"slave thread received Bad Gateway on host {job.hostname}")
                job.result = exc
                job.exception = True
            except Exception as exc:
                job.result = exc
                job.exception = True
                log.info(f"Exception recieved on host {job.hostname}")


            self.outputq.put(job)
            self.inputq.task_done()

    def submit(self, job):
        """ submit an job to this slave for processing """
        self.inputq.put(job)


    #def join():
    #    self.inputq.join()  # wait for the queue to be completed




# this is a hidden class
class SlaveProcess(object):
    def __init__(self, cluster, num_threads, outputq):
        self.cluster = cluster
        self.outputq = outputq
        self.queuesize = 0
        #self.inputq = multiprocessing.Queue()
        self.inputq = multiprocessing.JoinableQueue(100000) # 100,000 max entries?

        self.slavesthreads = list()
        self.num_threads = num_threads

        self.proc = multiprocessing.Process(target=self.slave_process, args=(cluster,), daemon=True)
        self.proc.start()


    def submit(self, job):
        """ submit an job to this slave for processing """
        self.inputq.put(job)
        self.queuesize += 1


    def slave_process(self, cluster):
        """ processes API call requests asychronously - runs in a sub-process (not thread) """
        self.slavethreads = list()
        self.thread_outputq =  multiprocessing.Queue(1000)
        self.bucket_array = list()


        #log.info(f"starting threads {time.asctime()}")
        # start sub-threads...  future?
        log.info(f"starting {self.num_threads} threads")
        for i in range(0, self.num_threads):
            self.slavesthreads.append(SlaveThread(self.cluster, self.outputq))
        #log.info(f"starting threads complete {time.asctime()}")

        slavestats = dict()
        hostname_tracker = dict()

        while True:
            #log.debug(f"waiting on queue")
            job = self.inputq.get()
            #log.debug(f"got job from queue, {job.hostname}, {job.category}, {job.stat}")
            if job.hostname is None:
                #self.log_stats()
                #log.info(f"thread bucket distribution: {slavestats}")
                #log.info(f"thread bucket hostname dist: {hostname_tracker}")
                return  # die_mf!

            hostobj = cluster.get_hostobj_byname(job.hostname)

            if hostobj is None:
                log.debug(f"error on hostname {job.hostname}, {job.parms}")

            # new stuff
            try:
                this_hash = self.bucket_array.index(job.hostname)
            except ValueError: 
                self.bucket_array.append(job.hostname)
                this_hash = self.bucket_array.index(job.hostname)

            bucket = this_hash % len(self.slavesthreads)

            #this_hash = hash(job.hostname)
            #bucket = this_hash % len(self.slavesthreads)
            #log.info(f"{job.hostname}/{this_hash}/{bucket}")

            if bucket not in slavestats:
                slavestats[bucket] = 1
            else:
                slavestats[bucket] += 1

            if job.hostname not in hostname_tracker:
                hostname_tracker[job.hostname] = bucket
            elif hostname_tracker[job.hostname] != bucket:
                log.info(f"bucket changed for {job.hostname} from {hostname_tracker[job.hostname]} to {bucket}")


            #log.info(f"thread bucket distribution: {slavestats}")
            self.slavesthreads[bucket].submit(job)
            self.inputq.task_done()

    def join():
        self.inputq.join()  # wait for the queue to be completed


# exposed class - distribute calls to SlaveProcess processes via input queues
class Async():
    def __init__(self, cluster, slave_procs=8, slave_thread_per_proc=100):
        self.cluster = cluster
        self.outputq = multiprocessing.Queue()
        self.slaves = list()
        self.num_outstanding = 0
        self.stats = dict()

        self.slaves = list()
        self.bucket_array = list()

        self.num_slaves = slave_procs
        self.slave_thread_per_proc = slave_thread_per_proc

        #log.info(f"starting processes {time.asctime()}")
        for i in range(0, self.num_slaves):
            self.slaves.append(SlaveProcess(self.cluster, self.slave_thread_per_proc, self.outputq))
        #log.info(f"starting processes complete {time.asctime()}")

    def __del__(self):
        die_mf = Job(None, None, None, None, None)
        for slave in self.slaves:
            slave.submit(die_mf)

    def submit(self, hostname, category, stat, method, parms):
        job = Job(hostname, category, stat, method, parms)      # wekahost?  Object? decisions, decisions
        try:
            this_hash = self.bucket_array.index(hostname)
        except ValueError: 
            self.bucket_array.append(hostname)
            this_hash = self.bucket_array.index(hostname)

        bucket = this_hash % len(self.slaves)
        #log.debug(f"{hostname}/{this_hash}/{bucket}")

        if bucket not in self.stats:
            self.stats[bucket] = 1
        else:
            self.stats[bucket] += 1

        #log.info(f"process bucket distribution: {self.stats}")

        self.slaves[bucket].submit(job)
        self.num_outstanding += 1

    def log_stats(self):
        log.info(f"process bucket distribution: {dict(sorted(self.stats.items()))}")

    def wait(self):
        """
        input q needs to be empty
        output q needs to be empty
        track in-flight api calls?
        """

        #self.log_stats()

        for slave in self.slaves:
            #log.error(f"joining slave queue {self.slaves.index(slave)}")
            slave.inputq.join()    # wait for the inputq to drain
            #slave.log_stats()


        while self.num_outstanding > 0:
            result = self.outputq.get()
            self.num_outstanding -= 1
            if not result.exception:
                if len(result.result) != 0:
                    yield result        # yield so it is an iterator
            else:
                log.debug(f"API sent error: {result.result}")
                # do we requeue?


if __name__ == "__main__":
    import time
    testme = Async()

    #testme.start()

    time.sleep(5)

    #testme.stop()




