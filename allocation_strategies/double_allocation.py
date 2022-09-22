import json

class DoubleAllocation:
    def __init__(self, stats):
        self.stats = stats
        self.prev_alloc = None

    def load_config(self):
        with open('allocation_strategies/double_allocation.cfg', 'r') as f:
            self.config = json.load(f)

    def add(self, p):
        pass

    def alloc(self, need_retry):
        if need_retry:
            new_alloc = {}
            if self.prev_alloc['cores'] * 2 <= self.max_cores:
                new_alloc['cores'] = self.prev_alloc['cores'] * 2
            else:
                new_alloc['cores'] = self.max_cores
            if self.prev_alloc['mem'] * 2 <= self.max_mem:
                new_alloc['mem'] = self.prev_alloc['mem'] * 2
            else:
                new_alloc['mem'] = self.max_mem
            if self.prev_alloc['disk'] * 2 <= self.max_disk:
                new_alloc['disk'] = self.prev_alloc['disk'] * 2
            else:
                new_alloc['disk'] = self.max_disk
        else:
            new_alloc = {'cores': self.base_cores, 'mem': self.base_mem, 'disk': self.base_disk}
        self.prev_alloc = new_alloc
        return new_alloc

    def run(self, data):
        self.load_config()
        self.max_cores = self.config['max_cores']
        self.max_mem = self.config['max_mem']
        self.max_disk = self.config['max_disk']
        self.base_cores = self.config['base_cores']
        self.base_mem = self.config['base_mem']
        self.base_disk = self.config['base_disk']
        i = 0
        for p in data:
            new_alloc = self.alloc(False)
            while not self.stats.accum(p, new_alloc, False):
                new_alloc = self.alloc(True)
            i += 1
            self.prev_alloc = None
        self.stats.display()
