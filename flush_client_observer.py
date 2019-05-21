import threading#multiprocessing
import time
from timeit import default_timer as timer
from service_communicator import Service_Contactor

class Flushing_CMonitor(threading.Thread):
    def __init__(self, 
                 givenShare, monitoring_duration, time_interval, 
                 edb_host = '127.0.0.1' , edb_port ='5000'):

        #super(Flushing_CMonitor, self).__init__()
        threading.Thread.__init__(self)
        
        self.cached_data_clusters  = givenShare
        self.monitoring_duration = monitoring_duration
        self.time_interval = time_interval
        
        self.service_connector = Service_Contactor(edb_host,edb_port)
                
        self.cache_size_list = list([])

    def run(self):
        count = int(self.monitoring_duration/self.time_interval) + 1
        self.service_connector.enable_edb_monitor(self.monitoring_duration,self.time_interval)
        
        for _ in range(count):
            track = self.cached_data_clusters.getClusterSizeAllWithoutLock()
            self.cache_size_list.append((timer(),
                                         track))

            time.sleep(self.time_interval)
            
        #print after period
        #self.service_connector.disable_edb_monitor()
        print("Monitored Cache " + str(self.cache_size_list))
        