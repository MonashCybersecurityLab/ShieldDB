import os
import utilities
# from app_controller import App_Controller
from cache_controller import Cache_Controller
from padding_controller import Padding_Controller
from multiprocessing.managers import BaseManager
from shared_cache import Orchestrator
import time

if __name__ == '__main__':

	alpha = 256#512
	padding_mode = "nh"
	monitoring_time = 150
	
	range_cache_size = [20000, 40000]

	#========================
	pds = ""
	if alpha == 256:
		pds = "data_padding_set_500"
		range_cache_size = [20000, 40000]

	if alpha == 512:
		pds = "data_padding_set_1200"
		range_cache_size = [40000, 80000]	
	
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
									 "localhost", 8089)

	padding_manager = Padding_Controller(givenShare,
										 fixed_clusters_keywords,
										 data_padding_set,
										 padding_mode, None,  # app.get_keys(),
										 "localhost", 8089,
										 '127.0.0.1' , '5000')

	print("Cache size %s" % str(cache_manager.getCacheSizes()))
	print("Padding size in total " + str(padding_manager.getPaddingDataSize()))
	print("Padding set (mb) " + str(os.path.getsize(os.path.join(key_folder, pds)) / (1024 * 1024.0)))
	print("Fixed cluster keywords size " + str(len(fixed_clusters_keywords)))
	print("Size of prob clusters " + str(len(prob_clusters)))
	
	cache_manager.start()
	padding_manager.start()
	cache_manager.join(timeout=monitoring_time)
	padding_manager.join(timeout=monitoring_time)
	padding_manager.terminate()  # while the padding has registered for stopping flag
	
	time.sleep(50)  # wait for writing log file
	
