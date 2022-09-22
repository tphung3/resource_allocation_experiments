import json
import bisect
import statistics
import random
import math 

class QuantileBucketing:
    def __init__(self, stats):
        self.stats = stats
        self.prev_alloc = None
        self.config = None 

        self.max_cores = 0
        self.max_mem = 0
        self.max_disk = 0
        self.num_cold = 0

        self.sorted_cores = []
        self.sorted_mem = []
        self.sorted_disk = []

        self.reps_cores = []
        self.reps_mem = []
        self.reps_disk = []

    def load_config(self):
        with open('allocation_strategies/quantile_bucketing.cfg', 'r') as f:
            self.config = json.load(f)

    def add(self, p):
        bisect.insort(self.sorted_cores, p['cores'])
        bisect.insort(self.sorted_mem, p['mem'])
        bisect.insort(self.sorted_disk, p['disk'])
           
    def alloc_one_res(self, need_retry, reps, prev_alloc_type, max_alloc):
        if need_retry:
            prev_alloc = self.prev_alloc[prev_alloc_type]
            start_idx = bisect.bisect(reps, prev_alloc)
            reps = reps[start_idx:]
            if len(reps) == 0:
                if self.prev_alloc[prev_alloc_type] * 2 < max_alloc:
                    return self.prev_alloc[prev_alloc_type] * 2
                return max_alloc
            alloc = random.sample(reps, k=1)[0]
        else:
            alloc = random.sample(reps, k=1)[0]
        return alloc

    def alloc(self, need_retry, is_cold, p):
        if is_cold:
            return {'cores': self.max_cores, 'mem': self.max_mem, 'disk': self.max_disk}
         
        if self.prev_alloc is None or p['cores'] > self.prev_alloc['cores']:
            cores = self.alloc_one_res(need_retry, self.reps_cores, 'cores', self.max_cores)
        else:
            cores = self.prev_alloc['cores']
        if self.prev_alloc is None or p['mem'] > self.prev_alloc['mem']:
            mem = self.alloc_one_res(need_retry, self.reps_mem, 'mem', self.max_mem)
        else:
            mem = self.prev_alloc['mem']
        if self.prev_alloc is None or p['disk'] > self.prev_alloc['disk']:
            disk = self.alloc_one_res(need_retry, self.reps_disk, 'disk', self.max_disk)
        else:
            disk = self.prev_alloc['disk']
        alloc = {'cores': cores, 'mem': mem, 'disk': disk}
        return alloc 

    def compute_one_res_quantiles(self, res_type):
        if res_type == 'cores':
            arr = self.sorted_cores
        elif res_type == 'mem':
            arr = self.sorted_mem
        elif res_type == 'disk':
            arr = self.sorted_disk
        reps = []
        for i in range(self.num_buckets):
            if i == self.num_buckets - 1:
                ind = -1
            else:
                ind = math.floor((i+1) / self.num_buckets * len(arr))
            reps.append(arr[ind])
        return reps
    
    def compute_quantiles(self):
        self.reps_cores = self.compute_one_res_quantiles('cores')
        self.reps_mem = self.compute_one_res_quantiles('mem')
        self.reps_disk = self.compute_one_res_quantiles('disk')

    def debug(self):
        print(self.reps_cores)
        print(self.reps_mem)
        print(self.reps_disk)
        print('-------------------\n')

    def run(self, data):
        self.load_config()
        self.max_cores = self.config['max_cores']
        self.max_mem = self.config['max_mem']
        self.max_disk = self.config['max_disk']
        self.num_cold = self.config['num_cold']
        self.num_buckets = self.config['num_buckets']

        for i, p in enumerate(data):
            #self.debug()
            if i == self.num_cold:
                self.compute_quantiles()
            new_alloc = self.alloc(False, i < self.num_cold, p)
            if i < self.num_cold:
                self.stats.accum(p, new_alloc, True)
                self.add(p)
            else:
                while not self.stats.accum(p, new_alloc, False):
                    self.prev_alloc = new_alloc
                    new_alloc = self.alloc(True, i < self.num_cold, p)
                self.add(p)
                self.compute_quantiles() 
            self.prev_alloc = None
        self.stats.display()
