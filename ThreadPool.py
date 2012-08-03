import threading
import Queue
import time
import sys

class ThreadPoolTask(threading.Thread):

    def __init__(self, queue, name = ""):
        threading.Thread.__init__(self)
        self.kill = False
        self.queue = queue
        self.name = name
        self._isRunning = False
        self._waitToKill = False

    def run(self):
        while not self.kill:
            self.isRunning = False
            try:
                callback, args = self.queue.get(timeout = 0.1)
                self.isRunning = True
                try:
                    callback(*args)
                except Exception, e:
                    print >> sys.stderr,"ThreadPoolTask [%s] :" % self.name, e
            except:
                pass
            if self._waitToKill and not self.isRunning:
                self.kill = True

    def stop(self):
        self.kill = True

    def waitAndStop(self):
        self._waitToKill = True

    def isKilled(self):
        return self.kill

class ThreadPool:

    def __init__(self, pool_size):
        self.pool_size = pool_size
        self.thread_list = []
        self.queue = Queue.Queue(pool_size)
        self.action_lock = threading.Lock()
        self._initThreads()

    def _initThreads(self):
        self.action_lock.acquire()
        for i in range(0, self.pool_size):
            thr = ThreadPoolTask(self.queue, "Thread " + str(i))
            self.thread_list.append(thr)
        for thr in self.thread_list:
            thr.start()
        self.action_lock.release()

    def _removeDeadThreads(self):
        for thr in self.thread_list:
            if thr.isKilled():
                self.thread_list.remove(thr)
                del thr

    def delThreads(self, num):
        try:
            self.action_lock.acquire()
            if self.thread_list == []:
                return
            for thr in self.thread_list:
                if num > 0:
                    thr.stop()
                    num -= 1
            self._removeDeadThreads()
        finally:
            self.action_lock.release()

    def addThreads(self, num):
        try:
            self.action_lock.acquire()
            self._removeDeadThreads()
            for cpt in range(num):
                thr = ThreadPoolTask(self.queue, "new Thread " + str(cpt))
                thr.start()
                self.thread_list.append(thr)
        except:
            pass
        finally:
            self.action_lock.release()

    def stopAll(self):
        for thr in self.thread_list:
            thr.stop()

    def waitAndStopAll(self):
        for thr in self.thread_list:
            thr.waitAndStop()

    def joinAll(self):
        for thr in self.thread_list:
            thr.join()

    def countThreads(self):
        try:
            self.action_lock.acquire()
            return len(self.thread_list)
        finally:
            self.action_lock.release()
 
    def addTask(self, callback, args):
        try:
            self.action_lock.acquire()
            self._removeDeadThreads()
            if self.thread_list == []:
                return
            self.queue.put((callback, args))
        finally:
            self.action_lock.release()



