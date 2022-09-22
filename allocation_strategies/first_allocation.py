import json
import bisect
import statistics
import random
import math 
import allocation_strategies.first_allocation_source.FirstAllocation as fa

class FirstAllocation:
    def __init__(self, stats):
        self.stats = stats
        self.prev_alloc = None
        self.config = None 

        self.max_cores = 0
        self.max_mem = 0
        self.max_disk = 0
        self.num_cold = 0
        
        self.fa_cores = fa.FirstAllocation('cores')
        self.fa_mem = fa.FirstAllocation('mem')
        self.fa_disk = fa.FirstAllocation('disk')

    def load_config(self):
        with open('allocation_strategies/quantile_bucketing.cfg', 'r') as f:
            self.config = json.load(f)

    def alloc_one_res(self, need_retry, fa_engine, prev_alloc_type, max_alloc):
        if need_retry:
            if fa_engine.maximum_seen == self.prev_alloc[prev_alloc_type]:
                return max_alloc
            return fa_engine.maximum_seen
        alloc = fa_engine.first_allocation('waste')
        #if prev_alloc_type == 'mem':
        #    print('Mem predicted', alloc)
        return alloc
        #return fa_engine.first_allocation('waste')

    def alloc(self, need_retry, is_cold, p):
        if is_cold:
            return {'cores': self.max_cores, 'mem': self.max_mem, 'disk': self.max_disk}
         
        if self.prev_alloc is None or p['cores'] > self.prev_alloc['cores']:
            cores = self.alloc_one_res(need_retry, self.fa_cores, 'cores', self.max_cores)
        else:
            cores = self.prev_alloc['cores']
        if self.prev_alloc is None or p['mem'] > self.prev_alloc['mem']:
            mem = self.alloc_one_res(need_retry, self.fa_mem, 'mem', self.max_mem)
        else:
            mem = self.prev_alloc['mem']
        if self.prev_alloc is None or p['disk'] > self.prev_alloc['disk']:
            disk = self.alloc_one_res(need_retry, self.fa_disk, 'disk', self.max_disk)
        else:
            disk = self.prev_alloc['disk']
        alloc = {'cores': cores, 'mem': mem, 'disk': disk}
        return alloc 

    def add(self, p):
        self.fa_cores.add_data_point(p['cores'], p['time'])
        self.fa_mem.add_data_point(p['mem'], p['time'])
        self.fa_disk.add_data_point(p['disk'], p['time'])

    def debug(self):
        print('-------------------\n')

    def run(self, data):
        self.load_config()
        self.max_cores = self.config['max_cores']
        self.max_mem = self.config['max_mem']
        self.max_disk = self.config['max_disk']
        self.num_cold = self.config['num_cold']

        for i, p in enumerate(data):
            #self.debug()
            #print(f'Task {i}')
            new_alloc = self.alloc(False, i < self.num_cold, p)
            if i < self.num_cold:
                self.stats.accum(p, new_alloc, True)
                self.add(p)
            else:
                #print(f'{i} {new_alloc} {self.fa_cores.maximum_seen} {self.fa_mem.maximum_seen} {self.fa_disk.maximum_seen}')
                while not self.stats.accum(p, new_alloc, False):
                    self.prev_alloc = new_alloc
                    new_alloc = self.alloc(True, i < self.num_cold, p)
                    #print(f'{i} {new_alloc} {self.fa_cores.maximum_seen} {self.fa_mem.maximum_seen} {self.fa_disk.maximum_seen}')
                self.add(p)
            self.prev_alloc = None
        self.stats.display()
