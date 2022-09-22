import statistics as stats

class AllocationStatisticsTracker:
    
    def __init__(self):
        
        #cores
        self.total_cores_t = 0  #1
        self.avg_total_cores_t = 0  #2
        self.no_cold_total_cores_t = 0  #3
        self.wcores_t = 0   #4
        self.avg_wcores_t = 0   #5
        self.no_cold_wcores_t = 0   #6
        self.wfail_alloc_core_t = 0 #7
        self.avg_wfail_alloc_core_t = 0 #8
        self.int_frag_core_t = 0    #9
        self.avg_int_frag_core_t = 0    #10
        self.cores_t_efficiency = 0 #11
        self.no_cold_cores_t_effi = 0   #12
        self.avg_cores_t_efficiency = 0 #13
        self.all_tasks_cores_efficiency = []    #14

        #mem
        self.total_mem_t = 0
        self.avg_total_mem_t = 0
        self.no_cold_total_mem_t = 0
        self.wmem_t = 0
        self.avg_wmem_t = 0
        self.no_cold_wmem_t = 0
        self.wfail_alloc_mem_t = 0
        self.avg_wfail_alloc_mem_t = 0
        self.int_frag_mem_t = 0
        self.avg_int_frag_mem_t = 0
        self.mem_t_efficiency = 0
        self.no_cold_mem_t_effi = 0
        self.avg_mem_t_efficiency = 0
        self.all_tasks_mem_efficiency = []
    
        #disk
        self.total_disk_t = 0
        self.avg_total_disk_t = 0
        self.no_cold_total_disk_t = 0
        self.wdisk_t = 0
        self.avg_wdisk_t = 0
        self.no_cold_wdisk_t = 0
        self.wfail_alloc_disk_t = 0
        self.avg_wfail_alloc_disk_t = 0
        self.int_frag_disk_t = 0
        self.avg_int_frag_disk_t = 0
        self.disk_t_efficiency = 0
        self.no_cold_disk_t_effi = 0
        self.avg_disk_t_efficiency = 0
        self.all_tasks_disk_efficiency = []

        #single task
        self.task_cores = 0
        self.task_mem = 0
        self.task_disk = 0

        #misc
        self.num_tasks = 0
        self.num_failed_tries = 0
        self.total_run_time = 0
        self.avg_run_time = 0

    def success_alloc(self, actual, predicted):
        if actual['cores'] > predicted['cores']:
            return False
        if actual['mem'] > predicted['mem']:
            return False
        if actual['disk'] > predicted['disk']:
            return False
        return True

    def accum(self, actual, predicted, in_cold_phase): 
        if self.success_alloc(actual, predicted):

            #cores
            self.total_cores_t += predicted['cores'] * actual['time']
            self.wcores_t += (predicted['cores'] - actual['cores']) * actual['time']
            self.int_frag_core_t += (predicted['cores'] - actual['cores']) * actual['time']

            #mem
            self.total_mem_t += predicted['mem'] * actual['time']
            self.wmem_t += (predicted['mem'] - actual['mem']) * actual['time']
            self.int_frag_mem_t += (predicted['mem'] - actual['mem']) * actual['time']

            #disk
            self.total_disk_t += predicted['disk'] * actual['time']
            self.wdisk_t += (predicted['disk'] - actual['disk']) * actual['time']
            self.int_frag_disk_t += (predicted['disk'] - actual['disk']) * actual['time']

            if not in_cold_phase:
                #cores
                self.no_cold_total_cores_t += predicted['cores'] * actual['time']
                self.no_cold_wcores_t += (predicted['cores'] - actual['cores']) * actual['time']

                #mem
                self.no_cold_total_mem_t += predicted['mem'] * actual['time']
                self.no_cold_wmem_t += (predicted['mem'] - actual['mem']) * actual['time']

                #disk
                self.no_cold_total_disk_t += predicted['disk'] * actual['time']
                self.no_cold_wdisk_t += (predicted['disk'] - actual['disk']) * actual['time']
            
            #single task
            self.all_tasks_cores_efficiency.append(actual['cores'] / (predicted['cores'] + self.task_cores))
            self.all_tasks_mem_efficiency.append(actual['mem'] / (predicted['mem'] + self.task_mem))
            self.all_tasks_disk_efficiency.append(actual['disk'] / (predicted['disk'] + self.task_disk))
            
            #misc
            self.num_tasks += 1
            self.total_run_time += actual['time']

            #reset
            self.task_cores = 0
            self.task_mem = 0
            self.task_disk = 0

            return True

        else:

            #cores
            self.total_cores_t += predicted['cores'] * actual['time']
            self.wcores_t += predicted['cores'] * actual['time']
            self.wfail_alloc_core_t += predicted['cores'] * actual['time']

            #mem
            self.total_mem_t += predicted['mem'] * actual['time']
            self.wmem_t += predicted['mem'] * actual['time']
            self.wfail_alloc_mem_t += predicted['mem'] * actual['time']

            #disk
            self.total_disk_t += predicted['disk'] * actual['time']
            self.wdisk_t += predicted['disk'] * actual['time']
            self.wfail_alloc_disk_t += predicted['disk'] * actual['time']

            if not in_cold_phase:
                #cores
                self.no_cold_total_cores_t += predicted['cores'] * actual['time']
                self.no_cold_wcores_t += predicted['cores'] * actual['time']

                #mem
                self.no_cold_total_mem_t += predicted['mem'] * actual['time']
                self.no_cold_wmem_t += predicted['mem'] * actual['time']

                #disk
                self.no_cold_total_disk_t += predicted['disk'] * actual['time']
                self.no_cold_wdisk_t += predicted['disk'] * actual['time']
           
            #single task
            self.task_cores += predicted['cores']
            self.task_mem += predicted['mem']
            self.task_disk += predicted['disk']

            #misc
            self.num_failed_tries += 1
            self.total_run_time += actual['time']
            
            return False

    def conclude(self):
        #cores
        self.avg_total_cores_t = self.total_cores_t / self.num_tasks
        self.avg_wcores_t = self.wcores_t / self.num_tasks
        self.avg_wfail_alloc_core_t = self.wfail_alloc_core_t / self.num_tasks
        self.avg_int_frag_core_t = self.int_frag_core_t / self.num_tasks
        self.cores_t_efficiency = 1 - self.wcores_t / self.total_cores_t
        self.no_cold_cores_t_effi = 1 - self.no_cold_wcores_t / self.no_cold_total_cores_t
        self.avg_cores_t_efficiency = stats.mean(self.all_tasks_cores_efficiency)

        #mem
        self.avg_total_mem_t = self.total_mem_t / self.num_tasks
        self.avg_wmem_t = self.wmem_t / self.num_tasks
        self.avg_wfail_alloc_mem_t = self.wfail_alloc_mem_t / self.num_tasks
        self.avg_int_frag_mem_t = self.int_frag_mem_t / self.num_tasks
        self.mem_t_efficiency = 1 - self.wmem_t / self.total_mem_t
        self.no_cold_mem_t_effi = 1 - self.no_cold_wmem_t / self.no_cold_total_mem_t
        self.avg_mem_t_efficiency = stats.mean(self.all_tasks_mem_efficiency)

        #disk
        self.avg_total_disk_t = self.total_disk_t / self.num_tasks
        self.avg_wdisk_t = self.wdisk_t / self.num_tasks
        self.avg_wfail_alloc_disk_t = self.wfail_alloc_disk_t / self.num_tasks
        self.avg_int_frag_disk_t = self.int_frag_disk_t / self.num_tasks
        self.disk_t_efficiency = 1 - self.wdisk_t / self.total_disk_t
        self.no_cold_disk_t_effi = 1 - self.no_cold_wdisk_t / self.no_cold_total_disk_t
        self.avg_disk_t_efficiency = stats.mean(self.all_tasks_disk_efficiency)

        #misc
        self.avg_run_time = self.total_run_time / self.num_tasks

    def display(self):
        self.conclude()
        print('\tabs\tavg')
        print(f'cores\t({self.cores_t_efficiency:.3f})\t({self.avg_cores_t_efficiency:.3f})')
        print(f'mem\t({self.mem_t_efficiency:.3f})\t({self.avg_mem_t_efficiency:.3f})')
        print(f'disk\t({self.disk_t_efficiency:.3f})\t({self.avg_disk_t_efficiency:.3f})')
        print(f'#failed\t{self.num_failed_tries}')
