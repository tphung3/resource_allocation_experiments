import argparse

from allocation_strategies.whole_machine import WholeMachine
from allocation_strategies.double_allocation import DoubleAllocation
from allocation_strategies.user_declaration import UserDeclaration
from allocation_strategies.k_means_bucketing import KMeansBucketing
from allocation_strategies.quantile_bucketing import QuantileBucketing
from allocation_strategies.first_allocation import FirstAllocation
from allocation_strategies.greedy_spick import GreedySpick
#from allocation_strategies.brute_force_spick import BruteForceSpick
from allocation_statistics import AllocationStatisticsTracker

#assume each algo has allocate and add methods
class Simulator:
    def __init__(self, dataset, algo, stats):
        self.dataset = dataset
        self.algo = algo 
        self.stats = stats
        self.max_alloc = {'cores': 0, 'mem': 0, 'disk': 0}

    def parse_line(self, line):
        res = line.split(" -- ")
        res_info = {}
        if res[0] == 'taskid':
            return None
        res_info['task_id'] = int(res[0])
        res_info['cores'] = int(res[1])
        res_info['mem'] = int(res[2])
        #res_info['virtual_memory'] = int(res[3])
        res_info['disk'] = int(res[4])
        res_info['time'] = round(float(res[5]), 5)
        res_info['average_cores'] = round(float(res[6]), 3)
        res_info['tag'] = int(res[7])
        if 'user_declaration' in self.algo:
            if self.max_alloc['cores'] < res_info['cores']:
                self.max_alloc['cores'] = res_info['cores']
            if self.max_alloc['mem'] < res_info['mem']:
                self.max_alloc['mem'] = res_info['mem']
            if self.max_alloc['disk'] < res_info['disk']:
                self.max_alloc['disk'] = res_info['disk']
        return res_info

    def load_data(self):
        data = []
        with open(self.dataset, 'r') as f:
            for line in f:
                res_info = self.parse_line(line)
                if res_info is None:
                    continue
                data.append(res_info)
        return data

    def pick_algo(self):
        algo_name = self.algo.split('/')[1].split('.py')[0]
        print(algo_name)
        if algo_name == 'whole_machine':
            algo = WholeMachine(self.stats)
        elif algo_name == 'double_allocation':
            algo = DoubleAllocation(self.stats)
        elif algo_name == 'user_declaration':
            algo = UserDeclaration(self.stats)
        elif algo_name == 'k_means_bucketing':
            algo = KMeansBucketing(self.stats)
        elif algo_name == 'quantile_bucketing':
            algo = QuantileBucketing(self.stats)
        elif algo_name == 'first_allocation':
            algo = FirstAllocation(self.stats)
        elif algo_name == 'brute_force_spick':
            algo = BruteForceSpick(self.stats)
        elif algo_name == 'greedy_spick':
            algo = GreedySpick(self.stats)
        return algo

    def run(self):
        data = self.load_data()
        algo = self.pick_algo()
        if 'user_declaration' in self.algo:
            algo.add_base_guess(self.max_alloc)
        algo.run(data)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', type=str, required=True)
    parser.add_argument('--algo', type=str, required=True)
    args = parser.parse_args()

    stats = AllocationStatisticsTracker()
    sim = Simulator(args.dataset, args.algo, stats)

    sim.run()
