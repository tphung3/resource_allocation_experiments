import random
import math
import json
import bisect
#QTask class, each qtask has consumption (cores, memory, disk), its task id, and its significance (a scalar)
class QTask:
    def __init__(self, consumption, task_id, significance):
        self.consumption = consumption
        self.task_id = task_id
        self.significance = significance

class GreedySpick:
    def __init__(self, stats):
        self.stats = stats
        self.prev_alloc = None
        self.config = None

        self.max_cores = 0
        self.max_mem = 0
        self.max_disk = 0
        self.num_cold = 0

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
        with open('allocation_strategies/greedy_spick.cfg', 'r') as f:
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

    def policy(self, p1, p2, delim_res, max_res, bot_arr, i, low_index, n1, n2, top_arr, high_index, arr_sig, sorted_res, bot_sig):

        #expectation of a new task consumption if it is below delim_res
        exp_cons_lq_delim = sum([sorted_res[j][0]*sorted_res[j][1]/bot_sig[high_index-low_index] for j in range(low_index, i+1)])

        #expectation of cost that new task is lower and our prediction is lower
        cost_lower_hit = p1*(p1*(delim_res-exp_cons_lq_delim))

        #expectation of cost that new task is lower and our prediction is higher
        cost_lower_miss = p1*(p2*(max_res-exp_cons_lq_delim))

        #if n2 == 0 then no upper bucket so no cost
        if n2 == 0:
            cost_upper_hit = 0
            cost_upper_miss = 0
        else:
            #expectation of a new task consumption if it is above delim_res
            exp_cons_g_delim = sum([sorted_res[j][0]*sorted_res[j][1]/bot_sig[high_index-low_index] for j in range(i+1, high_index+1)])

            #expectation of cost that new task is higher and our prediction is lower
            cost_upper_miss = p2*(p1*(delim_res+max_res-exp_cons_g_delim))

            #expectation of cost that new task is higher and our prediction is higher
            cost_upper_hit = p2*(p2*(max_res-exp_cons_g_delim))

        delim_cost = cost_lower_hit + cost_lower_miss + cost_upper_miss + cost_upper_hit
        return delim_cost

    def bucket_partitioning(self, low_index, high_index, sorted_res):
        
        #if there's only one element, return it
        if low_index == high_index:
            return [low_index]

        ret_arr = []                            #store indices of buckets' delimiters
                                                #(each bucket only has 1 delimiter)
        num_tasks = high_index - low_index + 1  #number of tasks in this segment
        sum_cons = 0                            #sum of consumption, a temporary variable
        sum_sig = 0                             #sum of significance, a tmp var
        arr_sig = [0]*num_tasks                 #array of all significance, sorted from low
                                                #to high as sorted_res (same ordering)
        bot_sig = [0]*num_tasks                 #sum of significance from bottom to i

        #update arr_sig
        for i in range(low_index, high_index+1):
            val_sig = sorted_res[i][1]
            arr_sig[i-low_index] = val_sig

        #update bot_arr and bot_sig
        bot_arr = [0]*num_tasks                 #sum of consumption from bottom to top
        for i in range(low_index,high_index+1):
            sum_cons += sorted_res[i][0]
            bot_arr[i-low_index] = sum_cons
            sum_sig += arr_sig[i-low_index]
            bot_sig[i-low_index] = sum_sig

        sum_cons = 0
        top_arr = [0]*num_tasks                 #sum of consumption from top to bottom

        #update top_arr
        for i in range(high_index-1, low_index-1, -1):
            sum_cons += sorted_res[i+1][0]
            top_arr[i-low_index] = sum_cons

        cost = -1                               #keep track of lowest cost
        ret_index = -1                          #keep track of index with lowest cost
        max_res = sorted_res[high_index][0]    #maximum resource in this segment

        #loop to calculate cost of partitioning at each point
        for i in range(low_index, high_index+1):
            delim_res = sorted_res[i][0]       #possible delimiter for new bucket
            n1 = i-low_index+1                          #number of tasks up to this delimiter
            n2 = num_tasks-n1                           #number of tasks above this delimiter, but in this segment
            p1 = bot_sig[i-low_index]/bot_sig[high_index-low_index]     #probability that a task falls into the below possible sub-bucket
            p2 = 1-p1                                                   #probability that a task falls into the above possible sub-bucket

            #calculate delim_cost according to policy
            delim_cost = self.policy(p1, p2, delim_res, max_res, bot_arr, i, low_index, n1, n2, top_arr, high_index, arr_sig, sorted_res, bot_sig)

            #track lowest cost and index of value that results in so
            if cost == -1 or cost > delim_cost:
                cost = delim_cost
                ret_index = i
            else:
                continue

        #if the index value having lowest cost is high_index, then return it as we don't do anymore bucket partitioning
        if ret_index == high_index:
            return [high_index]
        else:
            #get the arrays of indices from trying to divide the two sub-buckets
            result_low = self.bucket_partitioning(low_index, ret_index, sorted_res)
            result_high = self.bucket_partitioning(ret_index+1, high_index, sorted_res)
            ret_arr = result_low + result_high
        return ret_arr   
    
    def get_reps_res(self, sorted_res, bucket_partitions):
        last_index = -1
        ret = []
        for index in bucket_partitions:
            if last_index == -1:
                ret.append((sorted_res[index][0], (index+1)/len(sorted_res)))
            else:
                ret.append((sorted_res[index][0], (index-last_index)/len(sorted_res)))
            last_index = index
        return ret
####
    def get_reps(self):
        N = len(self.sorted_cores) - 1
        self.reps_cores = self.get_reps_res(self.sorted_cores, self.bucket_partitioning(0, N, self.sorted_cores))
        self.reps_mem = self.get_reps_res(self.sorted_mem, self.bucket_partitioning(0, N, self.sorted_mem))
        self.reps_disk = self.get_reps_res(self.sorted_disk, self.bucket_partitioning(0, N, self.sorted_disk))

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
