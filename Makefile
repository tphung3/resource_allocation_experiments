all:

test: test_whole_machine test_double_allocation test_user_declaration test_k_means_bucketing test_quantile_bucketing test_first_allocation test_greedy_spick

#dataset = bimodal_phase

dataset = colmena

test_whole_machine:
	python simulator.py --dataset resource_data/$(dataset)/data/resources_all.txt --algo allocation_strategies/whole_machine.py

test_double_allocation:
	python simulator.py --dataset resource_data/$(dataset)/data/resources_all.txt --algo allocation_strategies/double_allocation.py

test_user_declaration:
	python simulator.py --dataset resource_data/$(dataset)/data/resources_all.txt --algo allocation_strategies/user_declaration.py

test_k_means_bucketing:
	python simulator.py --dataset resource_data/$(dataset)/data/resources_all.txt --algo allocation_strategies/k_means_bucketing.py 

test_quantile_bucketing:
	python simulator.py --dataset resource_data/$(dataset)/data/resources_all.txt --algo allocation_strategies/quantile_bucketing.py

test_first_allocation:
	python simulator.py --dataset resource_data/$(dataset)/data/resources_all.txt --algo allocation_strategies/first_allocation.py

test_greedy_spick:
	python simulator.py --dataset resource_data/$(dataset)/data/resources_all.txt --algo allocation_strategies/greedy_spick.py

generate_synthetic_data: 
	python data_generation/generate_synthetic_data.py

clean: clean_synthetic_data clean_python_class

clean_synthetic_data:
	rm -r resource_data/synthetic/*

clean_python_class:
	rm allocation_strategies/*.pyc 
