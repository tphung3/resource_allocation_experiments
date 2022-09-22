import json

class WholeMachine:
    def __init__(self, stats):
        self.stats = stats

    def load_config(self):
        with open('allocation_strategies/whole_machine.cfg', 'r') as f:
            self.config = json.load(f)

    def add(self, p):
        pass

    def alloc(self):
        max_cores = self.config['max_cores']
        max_mem = self.config['max_mem']
        max_disk = self.config['max_disk']
        return {'cores': max_cores, 'mem': max_mem, 'disk': max_disk}


    def run(self, data):
        self.load_config()
        for p in data:
            self.stats.accum(p, self.alloc(), 0)
        self.stats.display()
