import json

class UserDeclaration:
    def __init__(self, stats):
        self.stats = stats

    def load_config(self):
        with open('allocation_strategies/user_declaration.cfg', 'r') as f:
            self.config = json.load(f)

    def add(self, p):
        pass

    def add_base_guess(self, base_alloc):
        self.base_alloc = base_alloc

    def alloc(self, need_retry):
        if need_retry:
            return self.max_alloc
        return self.base_alloc
        
    def run(self, data):
        self.load_config()
        self.max_alloc = {'cores': self.config['max_cores'], 'mem': self.config['max_mem'], 'disk': self.config['max_disk']}
        for p in data:
            new_alloc = self.alloc(False)
            if not self.stats.accum(p, new_alloc, 0):
                new_alloc = self.alloc(True)
                self.stats.accum(p, new_alloc, 0)
        self.stats.display()
