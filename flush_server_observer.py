import threading
import time
from timeit import default_timer as timer

class Flushing_SMonitor(threading.Thread):
    def __init__(self, monitoring_duration, time_interval):
        
        self.monitoring_duration = monitoring_duration
        self.time_interval = time_interval
        
        self.tracker = 0
    
        self.edb_size_list = list([])
        
        threading.Thread.__init__(self)
        
    def run(self):
        count = int(self.monitoring_duration/self.time_interval) + 1
        for _ in range(count):
            self.edb_size_list.append((timer(), self.tracker))

            time.sleep(self.time_interval)
            
        #print after period
        print("Monitored edb " + str(self.edb_size_list))