
"""
async.py - subprocesses to execute multithreaded API calls.
"""
import pickle
import multiprocessing
from logging import debug, info, warning, error, critical, getLogger, DEBUG
import threading
import wekalib
import time
import queue
import math

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
        self.times_in_q = 1


# this is a hidden class
class SlaveThread(object):
    def __init__(self, cluster, outputq):
        self.cluster = cluster
        self.outputq = outputq
        #self.inputq = multiprocessing.JoinableQueue(200) #  this used a LOT of semaphores; ran out of them
        self.inputq = queue.Queue();

        self.thread = threading.Thread(target=self.slave_thread, daemon=True)
        self.thread.start()

    def slave_thread(self):
        while True:
            try:
                job = self.inputq.get()
            except EOFError:
                del self.inputq
                return  # just silently die - this happens when the parent exits

            if job.hostname is None:
                # time to die
                del self.inputq
                return

            hostobj = self.cluster.get_hostobj_byname(job.hostname)
            try:
                job.result = hostobj.call_api(job.method, job.parms)
                job.exception = False
            except wekalib.exceptions.HTTPError as exc:
                if exc.code == 502:  # Bad Gateway - a transient error
                    log.error(f"slave thread received Bad Gateway on host {job.hostname}")
                    if job.times_in_q <= 10:
                        # retry a few times
                        job.times_in_q += 1
                        self.submit(job)
                        self.inputq.task_done()
                        continue    # go back to the inputq.get()
                    elif job.times_in_q <= 12:  # give it 2 more chances
                        # then sleep to give the cluster a little time to recover
                        time.sleep(0.5) # might be the only thing in the queue...
                        job.times_in_q += 1
                        self.submit(job)
                        self.inputq.task_done()
                        continue    # go back to the inputq.get()

                # else, give up and return the error - note: multiprocessing.queue module hates HTTPErrors - can't unpickle correctly
                job.result = wekalib.exceptions.APIError(f"{exc.host}: ({exc.code}) {exc.message}") # send as APIError
                job.exception = True
            except Exception as exc:
                job.result = exc
                job.exception = True
                log.info(f"Exception recieved on host {job.hostname}:{exc}")


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
        self.inputq = multiprocessing.JoinableQueue(50000) # 50,000 max entries?

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
        self.bucket_array = list()


        #log.info(f"starting threads {time.asctime()}")
        log.info(f"starting {self.num_threads} threads")
        for i in range(0, self.num_threads):
            self.slavesthreads.append(None) # reserve spots so we can start them on demand below
            #self.slavesthreads.append(SlaveThread(self.cluster, self.outputq))
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
                die_mf = Job(None, None, None, None, None)
                for slave in self.slavethreads:
                    slave.submit(die_mf)
                    slave.thread.join()    # wait for it to die
                del self.inputq
                return  # Goodbye, cruel world!

            hostobj = cluster.get_hostobj_byname(job.hostname)

            if hostobj is None:
                log.debug(f"error on hostname {job.hostname}, {job.parms}")

            # new stuff
            try:
                this_hash = self.bucket_array.index(job.hostname)
            except ValueError: 
                self.bucket_array.append(job.hostname)
                this_hash = self.bucket_array.index(job.hostname)

            bucket = this_hash % len(self.slavesthreads)    # if on-demand, how to hash?


            if bucket not in slavestats:
                slavestats[bucket] = 1
            else:
                slavestats[bucket] += 1

            if job.hostname not in hostname_tracker:
                hostname_tracker[job.hostname] = bucket
                # could start thread here on demand...
                # ie:
                #self.slavesthreads.append(SlaveThread(self.cluster, self.outputq))
                # need to make sure len(self.slavethreads) <= num_threads?
                # what happens if it's over num_threads?  or how to make sure it never exceeds num_threads?
            elif hostname_tracker[job.hostname] != bucket:
                log.info(f"bucket changed for {job.hostname} from {hostname_tracker[job.hostname]} to {bucket}")


            #log.info(f"thread bucket distribution: {slavestats}")
            if self.slavesthreads[bucket] is None:
                self.slavesthreads[bucket] = SlaveThread(self.cluster, self.outputq)    # start them on demand
            self.slavesthreads[bucket].submit(job)
            self.inputq.task_done()

    def join():
        self.inputq.join()  # wait for the queue to be completed


# exposed class - distribute calls to SlaveProcess processes via input queues
class Async():
    def __init__(self, cluster, max_procs=8, max_threads_per_proc=100):
        self.cluster = cluster
        self.outputq = multiprocessing.Queue()
        self.slaves = list()
        self.num_outstanding = 0
        self.stats = dict()

        self.slaves = list()
        self.bucket_array = list()

        #self.num_slaves = max_procs
        self.max_threads_per_proc = max_threads_per_proc

        # # of processes and threads to run...  (self-tuning)
        self.num_slaves = math.ceil(self.cluster.sizeof() / self.max_threads_per_proc)
        if self.num_slaves > max_procs:
            self.num_slaves = max_procs   # limit the number of slave processes we start

        #log.info(f"starting processes {time.asctime()}")
        for i in range(0, self.num_slaves):
            self.slaves.append(SlaveProcess(self.cluster, self.max_threads_per_proc, self.outputq))
        #log.info(f"starting processes complete {time.asctime()}")

    def __del__(self):
        die_mf = Job(None, None, None, None, None)
        for slave in self.slaves:
            slave.submit(die_mf)
            slave.proc.join()    # wait for it to die
        del self.outputq

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
            result = self.outputq.get() # need try/except here to prevent process from locking up
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




