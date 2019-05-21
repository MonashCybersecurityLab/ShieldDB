import copy, os
from multiprocessing import Lock
import utilities

# A Orchestrator is a proxying manager manages the shared cache data between processes.
# Orchestrator  is managed by an internal server process

class Orchestrator(object):
    def __init__(self,number_clusters):
        self.cached_data_clusters = [{} for _ in range(number_clusters)]
        self.lock = Lock()
        
    def getClusterByIndex(self,cluster_index): 
        self.lock.acquire()  
        img = copy.deepcopy(self.cached_data_clusters[cluster_index])
        self.lock.release()
        return img
    
    def getAllLocalCacheWithoutLock(self):
        self.lock.acquire()
        img = copy.deepcopy(self.cached_data_clusters)
        self.lock.release()
        utilities.dump_data_to_file(img, "tmp", "cache")
        content = self.getClusterSizeAllWithoutLock()
        print("Cache pairs " + str(content))
        print("Cache (mb) " + str(os.path.getsize(os.path.join("tmp", "cache"))/(1024*1024.0)))            

    def getClusterSizeByIndex(self,cluster_index):
        
        temp_size = 0
        self.lock.acquire() 
        for _,value in self.cached_data_clusters[cluster_index].items():
            temp_size += len(value)
        self.lock.release()        
        return temp_size
    
    def getClusterSizeAll(self):
        
        cur_size = []
        self.lock.acquire() 
        for cluster_index in range(len(self.cached_data_clusters)):
            temp_size = 0
            for _,value in self.cached_data_clusters[cluster_index].items():
                temp_size += len(value)
            cur_size.append(temp_size)
            
        self.lock.release()
        return cur_size
    
    def getClusterSizeAllWithoutLock(self):
        
        cur_size = []
        for cluster_index in range(len(self.cached_data_clusters)):
            temp_size = 0
            for _,value in self.cached_data_clusters[cluster_index].items():
                temp_size += len(value)
            cur_size.append(temp_size)
        
        #print("Test " + str(cur_size))
        return sum(cur_size)
           
    def getClusterKeyNum(self):
        cur_size = []
        self.lock.acquire() 
        for cluster_index in range(len(self.cached_data_clusters)):
            cur_size.append(len(self.cached_data_clusters[cluster_index]))
            
        self.lock.release()
        return cur_size
    
    def clearClusterByIndex(self,cluster_index):
        self.lock.acquire() 
        self.cached_data_clusters[cluster_index].clear()
        self.lock.release()

    def readCacheSizeAll(self):
        self.lock.acquire() 
        count = 0
        for cluster_index in range(len(self.cached_data_clusters)):
            count += len(self.cached_data_clusters[cluster_index])
        self.lock.release()
        return count

    def updateEntriesByDocument(self,keyword,docId,cluster_index):
        
        self.lock.acquire()
        if keyword in self.cached_data_clusters[cluster_index]:
            self.cached_data_clusters[cluster_index][keyword].add(docId)    
        else:
            self.cached_data_clusters[cluster_index][keyword] = set([docId])  
        self.lock.release()
        
    def updateClusterEntries(self, key_id_pairs): #(keyword,id,cluster_index)  
        self.lock.acquire()  
        for (keyword,docId,cluster_index) in key_id_pairs: 
            if keyword in self.cached_data_clusters[cluster_index]:
                self.cached_data_clusters[cluster_index][keyword].add(docId)    
            else:
                self.cached_data_clusters[cluster_index][keyword] = set([docId])                                   
        self.lock.release()
    
    def updateClusterByIndex(self, cluster_index,falling_back_data):
        self.lock.acquire()
        
        for keyword, ids in falling_back_data.items():
            if keyword in self.cached_data_clusters[cluster_index]:
                for docId in ids:
                    self.cached_data_clusters[cluster_index][keyword].add(docId)
            else:
                self.cached_data_clusters[cluster_index][keyword] = set([])
                for docId in ids:
                    self.cached_data_clusters[cluster_index][keyword].add(docId)
                                    
        self.lock.release()
        
    def search_local_cache(self, keyword):
        for cluster in self.cached_data_clusters:
            if keyword in cluster:
                return cluster[keyword]
        return None
