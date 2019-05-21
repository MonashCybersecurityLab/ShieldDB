import utilities
import multiprocessing
import os
import socket
from timeit import default_timer as timer


class Cache_Controller(multiprocessing.Process):
    
    def __init__(self, givenShare,
                 fixed_clusters_keywords,
                 prob_clusters,
                 range_cache_size,
                 streaming_dir, streaming_rate=10,
                 padding_mode="nh",
                 host="localhost", port=8089):
                                            
        super(Cache_Controller, self).__init__()

        self.number_clusters = len(fixed_clusters_keywords)
        self.clusters_keywords = fixed_clusters_keywords
        self.cluster_probabilities = prob_clusters
      
        # initThreshold for clusters
        self.cacheThresholdInit(range_cache_size)
                
        self.streaming_dir = streaming_dir  
        self.streaming_rate = streaming_rate
        self.padding_mode = padding_mode
        
        self.host = host
        self.port = port
        self.clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        
        self.cached_data_clusters = givenShare

        # enable flushing measurements and tracking
        if self.padding_mode[0] == "p":#this is p mode: adding one status
            self.eligible_pCluster = [0 for _ in range(self.number_clusters)]
            
    def initCacheByClustersProp(self, clusters_keywords_props, range_cache_size):
        
        if len(clusters_keywords_props) == 0:
            print("Invalid cache clusters")
            return None
        
        # parse cluster probs and keywords
        self.number_clusters = len(clusters_keywords_props)
                      
        # init the required keywords of each cluster
        self.clusters_keywords = [[] for _ in range(self.number_clusters)]
         
        # init prob of each cluster        
        self.cluster_probabilities = [0.0] * self.number_clusters
 
        for cluster_index in range(self.number_clusters):
            cluster = clusters_keywords_props[cluster_index]
            total_prob = 0.0
            for keyword_prob in cluster:
                self.clusters_keywords[cluster_index].append(keyword_prob[0])
                total_prob += keyword_prob[1]
                
            self.cluster_probabilities[cluster_index] = total_prob   
        
        # initThreshold for clusters
        self.cacheThresholdInit(range_cache_size)

    def cacheThresholdInit(self, range_cache_size):
        # initThreshold for clusters
        
        self.clusters_threshold = [0.0] * self.number_clusters
        
        m1 = min(self.cluster_probabilities)
        range1 = max(self.cluster_probabilities) - m1
    
        # normalise to [0,1]
        new_array = [(item - m1) / range1 for item in self.cluster_probabilities]
    
        # normalise to the scale of range_cache_size
        m2 = min(range_cache_size)
        range2 = max(range_cache_size) - m2
        
        self.clusters_threshold = [int((item * range2) + m2) for item in new_array]
       
    def getCacheSizes(self):   
        return self.clusters_threshold
            
    def exportClusters(self, datadir, cluster_keyword_file, cluster_prob_file):
        
        utilities.dump_data_to_file(self.clusters_keywords, datadir, cluster_keyword_file)
        utilities.dump_data_to_file(self.cluster_probabilities, datadir, cluster_prob_file)

    def search_local_cache(self, keyword):
        for cluster in self.cached_data_clusters:
            if keyword in cluster:
                return cluster[keyword]
        return None

    # cache controller will read data from the given dir ,every time grabs 10 files
    # this function perform in process to add data in to the cache
    # notify the Padding Controller via queuing process
    
    def run(self):
        
        try:
            print(">Activated: Cache Controller " + str((self.host, self.port)))
            
            self.clientSocket.connect((self.host, self.port))

            arr = os.listdir(self.streaming_dir)
            arr = [int(x) for x in arr]

            # read the data from source folder
            for i in range(1, len(arr), self.streaming_rate):
                # extract file sequence
                file_sequence = []
                if i + self.streaming_rate < len(arr):
                    file_sequence = [str(j) for j in range(i, i + self.streaming_rate)]
                else:
                    file_sequence = [str(j) for j in range(i, len(arr))]
               
                # select keyword/document pairs from these files
                cluster_map_pairs = []
                for fileId in file_sequence:
                    temp_pairs = utilities.retrieve_keywords_from_file(self.streaming_dir, fileId)
                    temp_cluster_map = self._keyword_cluster_map(temp_pairs)
                    cluster_map_pairs += temp_cluster_map
               
                # update cache clusters with these pairs
                self.cached_data_clusters.updateClusterEntries(cluster_map_pairs)
                # check against padding mode, either "n" or "p"
                cluster_indexes = []
                if self.padding_mode[0] == "n":
                    cluster_indexes = self._scanClusterByCapacity()
                else:
                    cluster_indexes = self._scanClusterByKeywords()

                # notify Padding Controller regarding the clusters satisfying the padding constraint
                for cluster_index in cluster_indexes:   
                    #if it is 0 make it 99, if it is for persistent- wait keywords, mark it as > 100             
                    self._broadcastClusterIndex(cluster_index)
                    # mark the first batch
                    if self.padding_mode[0] == "p":
                        self.eligible_pCluster[cluster_index] = 1
                                        
        except:
            # print("Exception Cache Controller")
            pass
        finally:          
            self.clientSocket.close()
    
    def _broadcastClusterIndex(self, cluster_index):
        # print ("Cluster index sent " + str(cluster_index))
        if cluster_index == 0:
            cluster_index = 99
            
        bindex = cluster_index.to_bytes(cluster_index.bit_length(), byteorder='little')
        self.clientSocket.send(bindex)
        # halts until response from Padding Controller
        # print("Cache controller is waiting")
        self.clientSocket.recv(1)
     
    def _keyword_cluster_map(self, temp_pairs):
        
        temp_cluster_map = []
        for (keyword, fileId) in temp_pairs:
            for cluster_index in range(len(self.clusters_keywords)):
                if keyword in self.clusters_keywords[cluster_index]:
                    temp_cluster_map.append((keyword, fileId, cluster_index))
        
        return temp_cluster_map
        
    def _initTimeCluster(self):
        for cluster_index in range(self.number_clusters):
            self.cluster_padding_time[cluster_index] = timer()
           
    def _scanClusterByCapacity(self):
        
        eligible_clusters = []
        
        cur_cluster_sizes = self.cached_data_clusters.getClusterSizeAll()
        
        for cluster_index in range(self.number_clusters):
            if cur_cluster_sizes[cluster_index] >= self.clusters_threshold[cluster_index]:
                eligible_clusters.append(cluster_index)
        
        return eligible_clusters
        
    def _scanClusterByKeywords(self):
        
        ready_clusters = []
        
        #scan by either keywords, or if it excess and it is eligible
        cur_cluster_keys = self.cached_data_clusters.getClusterKeyNum()
        
        for cluster_index in range(self.number_clusters):
            if cur_cluster_keys[cluster_index] == len(self.clusters_keywords[cluster_index]):
                ready_clusters.append(cluster_index)
            else:
                if self.eligible_pCluster[cluster_index] == 1:
                    #check current capacity 
                    cur_cluster_size = self.cached_data_clusters.getClusterSizeByIndex(cluster_index)
                    if cur_cluster_size  >= self.clusters_threshold[cluster_index]:
                        #mark cluster + 100 so that Padding Controller could add dummy keywords
                        ready_clusters.append(cluster_index + 100)
                        print("Catch the cluster " + str(cluster_index))
        return ready_clusters
        
