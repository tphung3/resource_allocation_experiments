import re
DATA_DIR = "/afs/crc.nd.edu/group/ccl/work/tphung/alarm-experiments-v2/resource_data/colmena/raw_data/"

id_to_type = {} #parsl task id to type
id_to_type2 = {} #parsl task id to type

with open(DATA_DIR+"parsl.log", 'r') as f:
	all_lines = f.readlines()
	for line in all_lines:
		if len(line.split(" launched on executor ")) > 1:
			parsl_task_id = int(line.split("Task ")[1].split(" launched on executor ")[0])
			task_type = line.split(" launched on executor ")[1].split("\n")[0]
			id_to_type[parsl_task_id] = task_type
		else:
			continue

with open(DATA_DIR+"parsl.log", 'r') as f:
	all_lines = f.readlines()
	for line in all_lines:
		if len(line.split("'task_executor'")) > 1:
			parsl_task_id = int(line.split("'task_id': ")[1].split(",")[0])
			task_type = line.split("'task_executor': '")[1].split("'")[0]
			id_to_type2[parsl_task_id] = task_type
		else:
			continue

with open(DATA_DIR+"parsl.log", 'r') as f:
	all_lines = f.readlines()
	for line in all_lines:
		if len(line.split("'task_executor'")) > 1:
			parsl_task_id = int(line.split("'task_id': ")[1].split(",")[0])
			task_type = line.split("'task_executor': '")[1].split("'")[0]
			id_to_type2[parsl_task_id] = task_type
		else:
			continue

#print(len(id_to_type), id_to_type)
print(len(id_to_type2), id_to_type2)
