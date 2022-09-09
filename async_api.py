
"""
async_api.py - subprocesses to execute multithreaded API calls.
"""
import pickle
import multiprocessing
from logging import debug, info, warning, error, critical, getLogger, DEBUG
import threading
import wekalib
import time
import queue
import math
import traceback
import json
import sys

# initialize logger - configured in main routine
log = getLogger(__name__)

# stupid macos doesn't fork by default
from multiprocessing import set_start_method
set_start_method("fork")

# this is a hidden class
class Job(object):
    def __init__(self, hostname, method, parms):
        self.hostname = hostname
        self.method = method
        self.parms = parms
        self.result = dict()
        self.exception = False
        self.times_in_q = 1

    def __str__(self):
        return f"{self.hostname},{json.dumps(self.result,indent=2)}"


die_mf = Job(None, None, None)

# this is a hidden class
class SlaveThread(object):
    def __init__(self, cluster, outputq):
        self.cluster = cluster
        self.outputq = outputq
        self.inputq = queue.Queue();

        self.thread = threading.Thread(target=self.slave_thread, daemon=True)
        self.thread.start()

    def __str__(self):
        return self.thread.name

    def slave_thread(self):
        while True:
            try:
                job = self.inputq.get()
            except EOFError:
                del self.inputq
                return  # just silently die - this happens when the parent exits

            log.debug(f"slave thread {self} received job hostname={job.hostname}")
            if job.hostname is None:
                # time to die
                log.debug(f"slave thread {self} told to die")
                del self.inputq
                return
            #else:
            #    log.debug(f"slave thread {self} received job")

            for retries in range(1,3):
                hostobj = self.cluster.get_hostobj_byname(job.hostname)
                try:
                    job.result = hostobj.call_api(job.method, job.parms)
                    job.exception = False
                    break
                except wekalib.exceptions.HTTPError as exc:
                    if exc.code == 502:  # Bad Gateway - a transient error
                        log.error(f"slave thread {self} received Bad Gateway on host {job.hostname}")
                        # retry a few times
                        log.debug(f"{self} retrying after Bad Gateway")
                        job.times_in_q += 1
                        #self.submit(job)
                        time.sleep(0.5) # make it yield so maybe the server will recover
                        #self.inputq.task_done()
                        job.result = wekalib.exceptions.APIError(f"{exc.host}: ({exc.code}) {exc.message}") # send as APIError
                        job.exception = True
                        continue    # go back to the inputq.get()

                except wekalib.exceptions.TimeoutError as exc:
                    job.result = exc
                    job.exception = True
                    log.error(f"{exc}")
                    break
                except Exception as exc:
                    job.result = exc
                    job.exception = True
                    log.info(f"Exception recieved on host {job.hostname}:{exc}")
                    log.info(traceback.format_exc())
                    break

            # else, give up and return the error - note: multiprocessing.queue module hates HTTPErrors - can't unpickle correctly
            #job.result = wekalib.exceptions.APIError(f"{exc.host}: ({exc.code}) {exc.message}") # send as APIError
            #job.exception = True
            if job.exception and type(job.result) is wekalib.exceptions.APIError:
                log.debug(f"{self} noting Bad Gateway exception, returning exception")


            # this will send back the above exeptions as well as good results
            #log.info(f"job.result={json.dumps(job.result, indent=2)}")
            log.debug(f"slave thread {self} queuing output.. Is exception = {job.exception}")
            self.outputq.put(job)
            #self.inputq.task_done()

    # slave thread submit
    def submit(self, job):
        """ submit an job to this slave for processing """
        self.inputq.put(job)


# Start a process that will have lots of threads
class SlaveProcess(object):
    def __init__(self, cluster, num_threads, outputq):
        self.cluster = cluster
        self.outputq = outputq
        self.queuesize = 0
        #self.inputq = multiprocessing.JoinableQueue(500) # 50,000 max entries?
        try:
            self.inputq = multiprocessing.Queue(500) # 50,000 max entries?
        except OSError as exc:
            log.critical(f"Cannot create Queue, {exc}, exiting")
            sys.exit(1)

        self.slavethreads = list()
        self.num_threads = num_threads

        # actually start the process
        self.proc = multiprocessing.Process(target=self.slave_process, args=(cluster,), daemon=True)
        self.proc.start()


    # process submit
    def submit(self, job):
        """ submit an job to this slave for processing """
        self.inputq.put(job)
        self.queuesize += 1

    def __str__(self):
        return self.proc.name


    def flush(self):
        self.inputq.close()
        self.inputq.join_thread()   # needed?

    # this is the main loop of the process created above
    def slave_process(self, cluster):
        """ processes API call requests asychronously - runs in a sub-process (not thread) """
        self.slavethreads = list()
        self.bucket_array = list()


        #log.info(f"starting threads {time.asctime()}")
        log.info(f"starting {self.num_threads} threads")
        for i in range(0, self.num_threads):
            self.slavethreads.append(None) # reserve spots so we can start them on demand below
            #self.slavethreads.append(SlaveThread(self.cluster, self.outputq))
        #log.info(f"starting threads complete {time.asctime()}")

        slavestats = dict()
        hostname_tracker = dict()

        while True:
            #log.debug(f"waiting on queue")
            job = self.inputq.get()
            #log.debug(f"got job from queue, {job.hostname}")

            log.debug(f"slave process {self} received job hostname={job.hostname}")
            if job.hostname is None:
                # we were told to die... shut it down
                log.debug(f"slave process {self} told to die {self.slavethreads}")
                for slave in self.slavethreads:
                    if slave is not None:
                        log.debug(f"sending die to {slave}")
                        slave.submit(die_mf)    # slave THREAD
                        # do we need to wait for the queue to drain?  Is that even a good idea?

                log.debug(f"{self} done telling threads")
                #while threading.active_count() > 0:
                #    log.debug(f"{threading.active_count()} threads are alive still")

                # have to leave a lot of time in case it has a full inputq?
                for slave in self.slavethreads:
                    if slave is not None:
                        log.debug(f"{self} joining thread {slave}")
                        slave.thread.join(timeout=30.0)    # wait for it to die
                        if slave.thread.is_alive():
                            log.error(f"a thread didn't die!")

                del self.inputq
                self.outputq.close()    # flush data to the ouputq
                self.outputq.join_thread()  # not sure if this is required or a bad idea
                return  # Goodbye, cruel world!
                # all are daemon threads, so when this process dies, so do all the threads

            #log.debug(f"slave process {self} received job")

            # check here to make sure we can get the host object; if not, toss the job - we won't be able to call the api anyway
            hostobj = cluster.get_hostobj_byname(job.hostname)

            if hostobj is None:
                log.debug(f"error on hostname {job.hostname}")
                job.result = wekalib.exceptions.APIError(f"{job.hostname}: (NOHOST) Host object not found") # send as APIError
                job.exception = True
                self.outputq.put(job)       # say it didn't work
                #self.inputq.task_done()     # complete the item on the inputq so parent doesn't hang
                continue

            # new stuff
            try:
                this_hash = self.bucket_array.index(job.hostname)
            except ValueError: 
                self.bucket_array.append(job.hostname)
                this_hash = self.bucket_array.index(job.hostname)

            bucket = this_hash % len(self.slavethreads)


            if bucket not in slavestats:
                slavestats[bucket] = 1
            else:
                slavestats[bucket] += 1

            if job.hostname not in hostname_tracker:
                hostname_tracker[job.hostname] = bucket
            elif hostname_tracker[job.hostname] != bucket:
                log.info(f"bucket changed for {job.hostname} from {hostname_tracker[job.hostname]} to {bucket}")


            # create a thread for the bucket, if needed
            if self.slavethreads[bucket] is None:
                log.debug(f"creating slave thread {bucket}")
                self.slavethreads[bucket] = SlaveThread(self.cluster, self.outputq)    # start them on demand

            # submit the job to a thread
            self.slavethreads[bucket].submit(job)
            #self.inputq.task_done()

    # process join
    def join(self):
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

        # create the slave processes
        for i in range(0, self.num_slaves):
            self.slaves.append(SlaveProcess(self.cluster, self.max_threads_per_proc, self.outputq))
        #log.info(f"starting processes complete {time.asctime()}")

    # kill the slave processes
    def __del__(self):
        #for slave in self.slaves:
        #    slave.submit(die_mf)
        #    slave.proc.join(5.0)    # wait for it to die (proc join)
        del self.outputq

    # submit a job
    def submit(self, hostname, method, parms):
        job = Job(hostname, method, parms)      # wekahost?  Object? decisions, decisions
        log.debug(f"submitting job {job}")
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

    # wait for all the API calls to return
    def wait(self):
        """
        input q needs to be empty
        output q needs to be empty
        track in-flight api calls?
        """

        #self.log_stats()

        log.debug(f"{threading.active_count()} threads active")

        #for slave in self.slaves:
        #    slave.proc.terminate()  # testing

        for slave in self.slaves:
            log.debug(f"sending die to {slave}")
            slave.submit(die_mf)    # tell the subprocess that we're done queueing tasks
            slave.flush()

        # inputq is a multiprocessing.JoinableQueue
        # outputq is a multiprocessing.Queue()
        # wait for inputq's to drain
        timed_out=False
        timeout_period = 30.0
        #timeout_period = 5.0 # testing
        while self.num_outstanding > 0:
            try:
                log.debug(f"outputq size is {self.outputq.qsize()}, num_outstanding is {self.num_outstanding}")
            except NotImplementedError:
                pass    # stupid macos returns error because qsize is broken in the OS

            try:
                result = self.outputq.get(True, timeout=timeout_period)   # don't block because they should be dead
            except queue.Empty as exc:
                # timed out - if timeout is specified, it either returns an item or queue.Empty on timeout
                log.error(f"outputq timeout!")  # should never happen because they're dead
                if not timed_out:
                    # we've timed out once already
                    timed_out = True
                    timeout_period = 0.01     # changing timeout period limits our max wait time
                self.num_outstanding -= 1   # just get rid of them
                continue

            self.num_outstanding -= 1
            if not result.exception:
                if len(result.result) != 0:
                    yield result        # yield so it is an iterator
            else:
                log.debug(f"API sent error: {result.result}")

        for slave in self.slaves:
            # wait for proc to finish processing inputq and die; timeout is 30s?
            log.debug(f"joining {slave}")
            slave.proc.join(0.1)    # don't wait for him, just kill him below
            log.debug(f"{slave} joined")
            if slave.proc.exitcode is None:
                # timed out - hung process!?
                log.debug(f"killing {slave}")
                if sys.version_info.minor < 7:
                    slave.proc.terminate()  # can't be rude
                else:
                    slave.proc.kill()   # we would rather be rude about it

        # queue should be empty now
        return


if __name__ == "__main__":
    import time
    testme = Async()

    #testme.start()

    time.sleep(5)

    #testme.stop()




