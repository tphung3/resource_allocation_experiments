import random
import math
import json
import bisect
import statistics as stats
from multiprocessing import Pool
from sklearn.cluster import KMeans

class BruteForceSpick:
    def __init__(self, stats):
        self.stats = stats
        self.prev_alloc = None
        self.config = None

        self.max_cores = 0
        self.max_mem = 0
        self.max_disk = 0
        self.num_cold = 0
        self.max_splits = 0
        self.max_kmeans_iter = 0

        self.sequence_cores = []    #(value, significance) pair #ex: (3,167)
        self.sequence_mem = []
        self.sequence_disk = []

        self.sorted_cores = [] #(value, significance) pair #ex: (3, 167)
        self.sorted_mem = []
        self.sorted_disk = []

        self.reps_cores = []    #(value, probability) pair #ex: (2, 0.33)
        self.reps_mem = []
        self.reps_disk = []

    def load_config(self):
        with open('allocation_strategies/brute_force_spick.cfg', 'r') as f:
            self.config = json.load(f)

    def sort_resources(self):
        key = lambda x : x[0]
        self.sorted_cores = sorted(self.sequence_cores, key=key)
        self.sorted_mem = sorted(self.sequence_mem, key=key)
        self.sorted_disk = sorted(self.sequence_disk, key=key)
    
    def add(self, p):
        self.sequence_cores.append((p['cores'], p['task_id']))
        self.sequence_mem.append((p['mem'], p['task_id']))
        self.sequence_disk.append((p['disk'], p['task_id']))
        self.sort_resources()

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
        return random.choices(pop, weights)[0]

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
        return {'cores': cores, 'mem': mem, 'disk': disk}

    #return indices to sorted_res
    def get_config(self, sorted_res, num_splits):
        sorted_res = [sorted_res[0] for i in range(len(sorted_res))]
        kmeans = KMeans(n_clusters=num_splits).fit(sorted_res)
        centroids = kmeans.cluster_centers_
        
        config = []
        for i in range(len(centroids) - 1):
            mid = (centroids[i+1] + centroids[i]) / 2
            idx = bisect.bisect(sorted_res, mid)
            config.append(mid - 1)
        config.append(len(sorted_res) - 1)

        return config

    def reweight(self, arr):
        ret = []
        s = sum(arr)

        for v in arr:
            ret.append(v/s)

        return ret

    #returns (cost, config associated w/ cost)
    #use k_means to determine split points
        
    #create table that calculate C[i,j], where C is cost, i is the index
    #of the bucket
    #where the true value of task is, and j is the index of the bucket
    #that we pick
    #adds all costs up and that's the true cost, return (cost, config from k_means)
        
    #remember to (1) estimate true value of task t to be E[i] (expectation/mean of all points
    #in bucket i, and (2) reweight probabilities at each C[i,j]
    def compute_cost(self, sorted_res, num_splits):

        config = self.get_config(sorted_res, num_splits)    #contain indices to sorted_res

        task_expectations = []
        bucket_probabilities = []
        start_idx = 0
        for i in range(num_splits):
            if start_idx == config[i]:
                task_expectations.append(sorted_res[start_idx])
                bucket_probabilities.append(1/len(sorted_res))
            else:
                task_expectations.append(stats.mean(sorted_res[start_idx: config[i]+1]))
                bucket_probabilities.append((config[i] - start_idx + 1)/len(sorted_res))
            start_idx = config[i]
        
        cost_table = [[0 for i in range(num_splits)] for j in range(num_splits)]
        
        #easy-to-fill indices
        for pick_bucket in range(num_splits):
            for task_bucket in range(pick_bucket+1):
                cost_table[task_bucket][pick_bucket] = sorted_res[config[pick_bucket]] - expectations[task_bucket]

        #rest of indices
        for task_bucket in range(num_splits, -1, -1):
            for pick_bucket in range(task_bucket, -1, -1):
                tmp_probabilities = self.reweight(bucket_probabilities[pick_bucket+1:num_splits])
                tmp_val = sorted_res[config[pick_bucket]]
                for i in range(pick_bucket+1, num_splits):
                    tmp_val += tmp_probabilities[i-pick_bucket-1] * cost_table[task_bucket][i]
                cost_table[task_bucket][pick_bucket] = tmp_val
        
        #compute expected cost
        expected_cost = 0
        for task_bucket in range(num_splits):
            for pick_bucket in range(num_splits):
                expected_cost += bucket_probabilities[task_bucket] * bucket_probabilities[pick_bucket] * cost_table[task_bucket][pick_bucket]

        #transform config to workable format
        config = [sorted_res[idx] for idx in config]
        
        return (cost, config)
        
    def get_reps_res(self, sorted_res):
        reps_res_pool = Pool(processes=self.max_splits)
        arr_results = []
        
        for num_splits in range(1, self.max_splits + 1):
            arr_results.append(pool.apply_async(compute_cost, [sorted_res, num_splits]))
        reps_res_pool.close()
        reps_res_pool.join()
        
        lowest_cost = -1
        best_config = None
        for i in range(self.max_splits):
            if lowest_cost == -1 or lowest_cost > arr_results[i][0]:
                lowest_cost = arr_results[i].get()[0]
                best_config = arr_results[i].get()[1]
        
        return best_config

    def get_reps(self):
        reps_pool = Pool(processes=3)
        result_reps_cores = pool.apply_async(get_reps_res, [self.sorted_cores])
        result_reps_mem = pool.apply_async(get_reps_res, [self.sorted_mem])
        result_reps_disk = pool.apply_async(get_reps_res, [self.sorted_disk])
        reps_pool.close()
        reps_pool.join()
       
        self.reps_cores = result_reps_cores.get()
        self.reps_mem = result_reps_mem.get()
        self.reps_disk = result_reps_disk.get()

    def trim_points(self):
        self.sequence_cores = self.sequence_cores[-500:]
        self.sequence_mem = self.sequence_mem[-500:]
        self.sequence_disk = self.sequence_disk[-500:]
        self.sort_resources()

    def too_many_points(self):
        return len(self.sorted_cores) >= 1000

    def debug(self):
        print(self.reps_cores)
        print(self.reps_mem)
        print(self.reps_disk)
        #print(self.sorted_cores)
        #print(self.sequence_cores)
        print(len(self.sorted_cores))
        print('----------------------------\n')

    def run(self, data):
        self.load_config()
        self.max_cores = self.config['max_cores']
        self.max_mem = self.config['max_mem']
        self.max_disk = self.config['max_disk']
        self.num_cold = self.config['num_cold']
        self.max_splits = self.config['max_splits']
        self.max_kmeans_iter = self.config['max_kmeans_iter']
        
        for i, p in enumerate(data):
            #print(f"Iteration {i}")
            #self.debug()
            if self.too_many_points():
                self.trim_points()
            if i == self.num_cold:
                self.get_reps()
            new_alloc = self.alloc(False, i < self.num_cold, p)
            if i < self.num_cold:
                self.stats.accum(p, new_alloc, True)
                self.add(p)
            else:
                while not self.stats.accum(p, new_alloc, False):
                    self.prev_alloc = new_alloc
                    new_alloc = self.alloc(True, i < self.num_cold, p)
                self.add(p)
                self.get_reps()
            self.prev_alloc = None
        self.stats.display()
