#This file generates synthetic datasets of resource consumption of tasks, varying by memory consumption only

#import relevant libraries
import numpy as np
import os
import matplotlib.pyplot as plt

#fix seed for consistent datasets
seed=20210602
np.random.seed(seed)

#set parameters
data_dir = os.path.abspath(".")+"/resource_data/"
num_tasks = 2000

#for machine capacity
default_machine_cores = 16
default_machine_mem = 64000
default_machine_disk = 64000 

#for tasks	
default_cores = 4	
default_disk = 4000		#in MBs
default_time = 500		#in seconds
default_virtual_memory = 0	#skipped
default_average_cores = 0	#skipped
default_tag = 1

#generate directories for datasets
def generate_data_dir(name, mem):
	
	#set resources to default values
	core = default_cores
	disk = default_disk
	time = default_time
	virtual_memory = default_virtual_memory
	average_cores = default_average_cores

	#check if dataset is already generated
	if not os.path.isdir(data_dir+name):

		#reformat memory value from (possibly) float to int
		mem_tag = [[int(val), int(tag)] for val, tag in mem]

		#make directories to store dataset in text file
		os.mkdir("{}{}".format(data_dir, name))
		os.mkdir("{}{}/data/".format(data_dir, name))

		#store dataset
		with open("{}{}/data/resources_all.txt".format(data_dir, name), 'w') as f:
	
			#first line with metadata for each task
			f.write("taskid -- core -- memory -- virtual_memory -- disk -- time -- average_cores -- tag\n")
			
			#start writing data points to file
			for i in range(num_tasks):
				memory = mem_tag[i][0]
				tag = mem_tag[i][1]

				#task_id start with 1
				data_point = "{} -- {} -- {} -- {} -- {} -- {} -- {} -- {}\n".format(i+1, core, memory, virtual_memory, disk, time, average_cores, tag)
				f.write(data_point)
		
		#print out notifying message
		print("Dataset {} generated at {}".format(name, data_dir))
	
	#skip if dataset was generated
	else:
		print("Dataset {} is already generated. Skipped.".format(name))

##sampling definitions start here

#generic normal, 1 type of tasks
def normal(mean, std, num_tasks):
	mem = np.random.normal(mean, std, num_tasks)
	mem_tag = [[i, default_tag] for i in mem]
	return mem_tag

#generic uniform, 1 type of tasks
def uniform(low, high, num_tasks):
	mem = np.random.uniform(low, high, num_tasks)
	mem_tag = [[i, default_tag] for i in mem]
	return mem_tag

#generic exponential, 1 type of tasks
def exponential(scale, size):
	mem = np.random.exponential(scale, size)
	for i in range(len(mem)):
		if mem[i] > default_machine_mem:
			mem[i] = default_machine_mem
	mem_tag = [[i, default_tag] for i in mem]
	return mem_tag

#generic beta, 1 type of tasks
def beta(a, b, maximum, size):
	mem = maximum*np.random.beta(a, b, size)
	mem_tag = [[i, default_tag] for i in mem]
	return mem_tag

#bimodal with two normals, 2 types of tasks, each type has equal number of data points
def bimodal(mean1, std1, mean2, std2, num_tasks, random):
	mem1 = np.random.normal(mean1, std1, num_tasks//2)
	mem2 = np.random.normal(mean2, std2, num_tasks//2)
	mem1_tag = [[i, default_tag] for i in mem1]
	mem2_tag = [[i, default_tag+1] for i in mem2]
	mem_tag = np.concatenate((mem1_tag, mem2_tag))
	if random == 1:
		np.random.shuffle(mem_tag)
	return mem_tag

#trimodal with three normals, 3 types of tasks, each type has (almost) equal number of data points
def trimodal(mean1, std1, mean2, std2, mean3, std3, num_tasks, random):
	mem1 = np.random.normal(mean1, std1, num_tasks//3)
	mem2 = np.random.normal(mean2, std2, num_tasks//3)
	mem3 = np.random.normal(mean3, std3, num_tasks - 2*(num_tasks//3))
	mem1_tag = [[i, default_tag] for i in mem1]
	mem2_tag = [[i, default_tag+1] for i in mem2]
	mem3_tag = [[i, default_tag+2] for i in mem3]
	mem_tag = np.concatenate((mem1_tag, mem2_tag, mem3_tag))
	if random == 1:
		np.random.shuffle(mem_tag)
	return mem_tag

#uniform but with number of task types = num_classes argument, each type has (almost) equal number of data points
def uniform_same(low, high, num_classes, num_tasks):
	all_mem = []
	for num in range(num_classes):
		if num == num_classes - 1:
			mem = np.random.uniform(low, high, num_tasks - (num_classes-1)*num_tasks//num_classes)
		else:
			mem = np.random.uniform(low, high, num_tasks//num_classes)
		mem_tag = [[i, default_tag+num] for i in mem]
		all_mem.append(mem_tag)
	all_mem_tag = []
	for arr in all_mem:
		for pair in arr:
			all_mem_tag.append(pair)
	np.random.shuffle(all_mem_tag)
	return all_mem_tag

##sampling definitions end here

#generate synthetic datasets by generating directories and then the datasets
generate_data_dir("normal_large", normal(mean=32000, std=5000, num_tasks=num_tasks))
generate_data_dir("normal_small", normal(mean=8000, std=2000, num_tasks=num_tasks))
generate_data_dir("uniform_large", uniform(low=10000, high=40000, num_tasks=num_tasks))
generate_data_dir("uniform_small", uniform(low=1000, high=4000, num_tasks=num_tasks))
generate_data_dir("exponential", exponential(scale=20000, size=(num_tasks)))
generate_data_dir("beta", beta(a=8, b=2, maximum=40000, size=num_tasks))
generate_data_dir("inverse_beta", beta(a=2, b=5, maximum=40000, size=num_tasks))
generate_data_dir("bimodal", bimodal(mean1=32000, std1=5000, mean2=8000, std2=2000, num_tasks=num_tasks, random=1))
generate_data_dir("trimodal", trimodal(mean1=32000, std1=4000, mean2=11000, std2=1000, mean3=16000, std3=4000, num_tasks=num_tasks, random=1))
generate_data_dir("bimodal_small_std", bimodal(mean1=32000, std1=500, mean2=8000, std2=200, num_tasks=num_tasks, random=1))
generate_data_dir("trimodal_small_std", trimodal(mean1=32000, std1=500, mean2=11000, std2=500, mean3=16000, std3=500, num_tasks=num_tasks, random=1))
generate_data_dir("bimodal_same", bimodal(mean1=8000, std1=2000, mean2=8000, std2=2000, num_tasks=num_tasks, random=1))
generate_data_dir("uniform_same", uniform_same(low=8000, high=9000, num_classes=4, num_tasks=num_tasks))
generate_data_dir("exponential_small", exponential(scale=10000, size=(num_tasks)))
generate_data_dir("bimodal_phase", bimodal(mean1=32000, std1=500, mean2=8000, std2=200, num_tasks=num_tasks, random=0))
generate_data_dir("trimodal_phase", trimodal(mean1=32000, std1=500, mean2=11000, std2=500, mean3=16000, std3=500, num_tasks=num_tasks, random=0))
generate_data_dir("bimodal_phase_inverted", bimodal(mean1=8000, std1=500, mean2=32000, std2=200, num_tasks=num_tasks, random=0))
generate_data_dir("trimodal_phase_incremental", trimodal(mean1=11000, std1=500, mean2=16000, std2=500, mean3=32000, std3=500, num_tasks=num_tasks, random=0))
###FOR TESTING PURPOSES
num_tasks = 20
generate_data_dir("test", uniform(low=10000, high=20000, num_tasks=num_tasks))

