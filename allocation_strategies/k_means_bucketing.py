import json
import bisect
import statistics
import random
import math 

class KMeansBucketing:
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
        
        self.means_cores = []
        self.means_mem = []
        self.means_disk = []

        self.reps_cores = []
        self.reps_mem = []
        self.reps_disk = []

    def load_config(self):
        with open('allocation_strategies/k_means_bucketing.cfg', 'r') as f:
            self.config = json.load(f)

    #assume means are filled
    def get_cluster_reps_and_probs(self, res_type):
        reps = []
        if res_type == 'cores':
            arr = self.sorted_cores
            means = self.means_cores
        elif res_type == 'mem':
            arr = self.sorted_mem
            means = self.means_mem
        elif res_type == 'disk':
            arr = self.sorted_disk
            means = self.means_disk
    
        rep_pos = -1
        for i in range(self.num_buckets - 1):
            m1 = means[i]
            m2 = means[i + 1]
            mid = (m1 + m2) / 2
            lo = rep_pos + 1
            rep_pos = rep_pos + bisect.bisect(arr[rep_pos + 1:], mid)
            rep = arr[rep_pos]
            hi = rep_pos
            reps.append((rep, (hi - lo + 1)/len(arr)))    #rep, prob of bucket
            if rep_pos == len(arr) - 1:
                while len(reps) < self.num_buckets:
                    reps.append((arr[-1], 1/len(arr)))
                return reps
        reps.append((arr[-1], (len(arr) - rep_pos - 1)/len(arr)))
        return reps

    def compute_means(self, res_type):
        if res_type == 'cores':
            arr = self.sorted_cores
            reps = self.reps_cores
        elif res_type == 'mem':
            arr = self.sorted_mem
            reps = self.reps_mem
        elif res_type == 'disk':
            arr = self.sorted_disk
            reps = self.reps_disk

        new_means = []
        if len(reps) == 0:
            for i in range(self.num_buckets):
                if i == self.num_buckets - 1:
                    ind = -1
                else:
                    ind = math.floor((i+1) / self.num_buckets * len(arr))
                new_means.append(arr[ind])
            return new_means

        rep_pos = -1
        for i in range(self.num_buckets):
            rep = reps[i][0]
            prev_rep_pos = rep_pos
            rep_pos = prev_rep_pos + bisect.bisect(arr[prev_rep_pos + 1:], rep)
            if rep_pos == prev_rep_pos:
                new_means.append(arr[rep_pos])
            else:
                new_means.append(statistics.mean(arr[prev_rep_pos + 1 : rep_pos + 1]))

            if rep_pos == len(arr) - 1:
                break
        while len(new_means) < self.num_buckets:
            new_means.append(arr[-1])
        return new_means
    
    def compute_all_means(self):
        self.means_cores = self.compute_means('cores')
        self.means_mem = self.compute_means('mem')
        self.means_disk = self.compute_means('disk')

    def get_clusters(self):
        self.reps_cores = self.get_cluster_reps_and_probs('cores')
        self.reps_mem = self.get_cluster_reps_and_probs('mem')
        self.reps_disk = self.get_cluster_reps_and_probs('disk')

    def add(self, p):
        bisect.insort(self.sorted_cores, p['cores'])
        bisect.insort(self.sorted_mem, p['mem'])
        bisect.insort(self.sorted_disk, p['disk'])
           
    def alloc_one_res(self, need_retry, reps, prev_alloc_type, max_alloc):
        pop = [reps[i][0] for i in range(len(reps))]
        weights = [reps[i][1] for i in range(len(reps))]
        if need_retry:
            prev_alloc = self.prev_alloc[prev_alloc_type]
            start_idx = bisect.bisect(pop, prev_alloc)
            pop = pop[start_idx:]
            if len(pop) == 0:
                if self.prev_alloc[prev_alloc_type] * 2 < max_alloc:
                    return self.prev_alloc[prev_alloc_type] * 2
                return max_alloc
            weights = weights[start_idx:]
            alloc = random.choices(pop, weights)[0]
        else:
            alloc = random.choices(pop, weights)[0]
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

    def debug(self):
        print(self.means_cores)
        print(self.reps_cores)
        print(self.means_mem)
        print(self.reps_mem)
        print(self.means_disk)
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
                self.compute_all_means()
                self.get_clusters()
            new_alloc = self.alloc(False, i < self.num_cold, p)
            if i < self.num_cold:
                self.stats.accum(p, new_alloc, True)
                self.add(p)
            else:
                while not self.stats.accum(p, new_alloc, False):
                    self.prev_alloc = new_alloc
                    new_alloc = self.alloc(True, i < self.num_cold, p)
                self.add(p)
                self.compute_all_means()
                self.get_clusters()
            self.prev_alloc = None
        self.stats.display()
