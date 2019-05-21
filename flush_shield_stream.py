import os, sys
import utilities
# from app_controller import App_Controller
from flush_cache_controller import Cache_Controller
from padding_controller import Padding_Controller
from multiprocessing.managers import BaseManager
from flush_shared_cache import Orchestrator
from flush_client_observer import Flushing_CMonitor
import time

if __name__ == '__main__':

	alpha = 256
	padding_mode = "nh" #only works for non-persistent
	monitoring_time = 150
	range_cache_size = [20000, 40000]

	#========================
	pds = ""
	if alpha == 256:
		pds = "data_padding_set_500"
		range_cache_size = [20000, 40000]

	if alpha == 512:
		pds = "data_padding_set_1500"
		range_cache_size = [40000, 80000]  # otherwise, having bottleneck		
	
	# if this is f_mode, we need to monitor for more than 30 seconds
	f_period = monitoring_time + 150
	f_interval = 10  # seconds
	f_window = 50  # 20
	
	proj_dir = os.getcwd()
	streaming_dir = os.path.join(proj_dir, "streaming")
	streaming_rate = 10
	
	print(">>>Processing " + str(alpha) + "_" + padding_mode + "_" + str(monitoring_time)) 

	# app = App_Controller()
	
	# store temp data
	temp_folder = "tmp"
	db_folder = "shield.db"
	utilities.resetFolders([temp_folder, db_folder])

	key_folder = "data" + str(alpha)
	data_padding_set = utilities.load_data_from_file(key_folder, pds)
	fixed_clusters_keywords = utilities.load_data_from_file(key_folder, "fixed_clusters_keywords")
	prob_clusters = utilities.load_data_from_file(key_folder, "prob_clusters")         

	BaseManager.register('Orchestrator', Orchestrator) 
	manager = BaseManager()
	manager.start()

	givenShare = manager.Orchestrator(len(fixed_clusters_keywords))

	cache_manager = Cache_Controller(givenShare,
									 fixed_clusters_keywords,
									 prob_clusters,
									 range_cache_size,
									 streaming_dir, streaming_rate,
									 padding_mode,
									 "localhost", 8089,
									 f_window)

	padding_manager = Padding_Controller(givenShare,
										 fixed_clusters_keywords,
										 data_padding_set,
										 padding_mode, None,  # app.get_keys(),
										 "localhost", 8089,
										 '127.0.0.1' , '5000')
	  
	print("Adaptive cache size %s" % str(cache_manager.getCacheSizes()))
	print("Padding size in total " + str(padding_manager.getPaddingDataSize()))
	print("Padding set (mb) " + str(os.path.getsize(os.path.join(key_folder, pds)) / (1024 * 1024.0)))

	# start Cache and Padding Controller process
	cache_manager.start()
	padding_manager.start()

	f_monitor = Flushing_CMonitor(givenShare, f_period, f_interval)
	f_monitor.start()
	cache_manager.join(timeout=monitoring_time)
	padding_manager.join(timeout=monitoring_time)
	padding_manager.terminate()  # while the padding has registered for stopping flag

	time.sleep(50)  # wait for writing
	