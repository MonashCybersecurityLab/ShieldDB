import utilities
import random
import multiprocessing
import socket
import copy
import sys
from sse_client import SSE_Client
from service_communicator import Service_Contactor
from signal import signal, SIGTERM
from timeit import default_timer as timer


class Padding_Controller(multiprocessing.Process):
    
    def __init__(self, givenShare,
                 fixed_clusters_keywords,
                 ds,
                 padding_mode="nh", keys=None,
                 controller_host="localhost", controller_port=8089,
                 edb_host='127.0.0.1' , edb_port='5000'):
        
        super(Padding_Controller, self).__init__()
        
        self.padding_ds = ds
        
        self.padding_mode = padding_mode

        self.host = controller_host
        self.port = controller_port
        
        self.serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.serverSocket.bind((self.host, self.port))
        self.serverSocket.listen(10)
        
        self.cached_data_clusters = givenShare
        self.clusters_keywords = fixed_clusters_keywords
        
        self.keywords_tracking = {}  # containing real and padding documents for each keyword
    
        self.accumulated_throughput = 0
        self.accumulated_bogus = 0
        self.batch_count = 0
        
        self.sse_client = SSE_Client(keys)
        self.service_connector = Service_Contactor(edb_host, edb_port)
        self.service_connector.open_connection()
        
        self.total_time_pad_enc = 0.0
        
        signal(SIGTERM, self.stop)
        self.flag_exit = False
          
    def generatePaddingData(self, clusters_keywords_props, max_bogus_size=550000, **kwargs):
        # format [[('a',0.23),('b',0.2),...], [], [], [], []  ]
        
        file_count = [(100000 + i) for i in range(1, max_bogus_size + 1)]
        self.padding_ds = {}
            
        for cluster in clusters_keywords_props:
            for keyword_prop in cluster:
                keyword = keyword_prop[0]
                probability = keyword_prop[1]
                
                bogus_file_no = int(probability * max_bogus_size * 100)  # 100 is average document/file
                
                random.shuffle(file_count)
            
                bogus_ids = file_count[0:bogus_file_no]
                self.padding_ds[keyword] = bogus_ids
                
        # dump to file
        if kwargs['writable'] == True:
            utilities.dump_data_to_file(self.padding_ds, kwargs['dir'], kwargs['file_name'])
        
    def run(self):
        try:
            print(">Activated: Padding Controller " + str((self.host, self.port)))      
            
            # halt and wait for coming connection
            connection, _ = self.serverSocket.accept()

            while self.flag_exit == False:
                buf = connection.recv(10)  # recv is waiting for data
                cluster_index = int.from_bytes(buf, byteorder='little')
                if cluster_index == 99:
                    cluster_index = 0 
                # print ("Cluster index received " + str(cluster_index))
                
                # assum > 100 is for flushing clusters of persistent
                # add to the cached all keywords that do not appear in the above list against the keywords of clusters 
                # add them to the same cached data for padding
            
                if self.flag_exit == False:
                    cached_data = dict()
                    
                    start_time = timer()
                    
                    if cluster_index < 100:
                        # retrieve cluster data from the cluster
                        cached_data = self.cached_data_clusters.getClusterByIndex(cluster_index)
                        # then empty the cluster
                        self.cached_data_clusters.clearClusterByIndex(cluster_index)

                    else:  # which means special clusters for flushing by persistent
                        # retrieve cluster data from the cluster
                        cached_data = self.cached_data_clusters.getClusterByIndex(cluster_index - 100)
                        # then empty the cluster
                        self.cached_data_clusters.clearClusterByIndex(cluster_index - 100)                       
                        # add dummy keywords of the cluster, which have not occurred, into this cached_data
                        cached_data = self.addBogusKeywords(cached_data, cluster_index - 100)                       
                    
                    if cached_data is None:
                        break
                        
                    # perform padding 
                    padded_data, falling_back_data, batch_throughput, batch_padd_count = self.padCachedClusterData(cached_data)
                    
                    if padded_data is None:
                        break
                        
                    if self.flag_exit == False:
                        # put data back to shared cache cluster data if possible
                        if cluster_index >= 100:
                            cluster_index -= 100
                        
                        self.cached_data_clusters.updateClusterByIndex(cluster_index, falling_back_data)
                        
                        # accumulate tracking
                        self.accumulated_throughput += batch_throughput
    
                        self.accumulated_bogus += batch_padd_count
                        self.batch_count += 1
    
                        # then hand over padded_data to SSE Client
                        encrypted_batch = self.sse_client.streaming(padded_data)
                        padded_data = None
                        
                        #record the ending time
                        end_time = timer()
                        self.total_time_pad_enc += end_time - start_time
                        
                        # transfer the encrypted_batch to server
                        if self.flag_exit == False:                        
                            self.service_connector.streaming_connect(encrypted_batch)
                        
                        # notify back CacheController
                        connection.send(bytes('1', 'utf-8'))                    
                else:
                    break
    
        except:
            # print("Exception in Padding Controller")
            pass
        finally:
            connection.close()

    def stop(self, *args):
        self.flag_exit = True  # important to change
        self.dumpData()
        self.serverSocket.close()
        self.service_connector.close_connection()
  
    def dumpData(self):

            print("Start logging data to tmp folder")    
            # ClientState
            # utilities.dump_data_to_file(self.padding_ds,"tmp", "paddingset")
            utilities.dump_data_to_file(self.getClientState(), "tmp", "client")
            self.cached_data_clusters.getAllLocalCacheWithoutLock()
            # utilities.dump_data_to_file(self.cached_data_clusters.getAllLocalCacheWithoutLock(), "tmp", "cache")
            utilities.dump_data_to_file(self.keywords_tracking, "tmp", "keywords_tracking")
            
            # print out 
            # throughput, bogus pairs, local cache pairs and cache size, 
            # total pairs in edb, and average result length in paddedDB
            
            print("Throughput " + str(self.accumulated_throughput))
            print("Batch count " + str(self.batch_count)) 
            print("Average batch processing time (ms) " + str((self.total_time_pad_enc/self.batch_count )*1000))
            print("Bogus pairs " + str(self.accumulated_bogus))
            # content = self.cached_data_clusters.getClusterSizeAllWithoutLock()
            # print("Cache pairs " + str(content))
            # print("Cache (mb) " + str(os.path.getsize(os.path.join("tmp", "cache"))/(1024*1024.0)))
            
            print("EDB size " + str(utilities.monitor_dict_size(self.keywords_tracking)))
            print("EDB size (mb) " + str(utilities.get_size("shield.db") / (1024 * 1024.0)))        
            print("Avg result length " + str(utilities.monitor_dict_size(self.keywords_tracking) / len(self.keywords_tracking)))

    def getClientState(self):
        return self.sse_client.exportClientState()
        
    def getThroughput(self):
        return self.accumulated_throughput
    
    def getBogus(self):
        return self.accumulated_bogus
     
    def getBatchCount(self):
        return self.batch_count 

    def getPaddingDataSize(self):
        size = 0
        for _, value in self.padding_ds.items():
            size += len(value)
    
        return size
    
    def addBogusKeywords(self, cached_data, cluster_index):    
        
        # add compliment
        for key in self.clusters_keywords[cluster_index] :
            if key not in cached_data:
                cached_data[key] = set([]) 
    
        return cached_data
    
    def padCachedClusterData(self, cached_data):
        # cache_data is a dict of {'a':[],'b':[]}, giving padding mode 
        # padded_data, falling_back_data, cur_throughput, cur_padd_count
        
        if cached_data is None:
            return None, None, None, None
        
        batch_throughput_num = 0
        batch_padding_num = 0
        
        if self.padding_mode[1] == "h":  # high_padding
            # identify the max_length of current keywords in the cached_data 
            max_length = 0 
            for _, value in cached_data.items():                
                if max_length < len(value):
                    max_length = len(value)
                        
            # cross check if such max_length causes unique_result_length in EDB 
            # via keywors_trackings
            padded_max_length = 0
            for keyword in cached_data:
                if keyword in self.keywords_tracking:
                    cur_length = self.keywords_tracking[keyword] + max_length
                    if cur_length > padded_max_length:
                        padded_max_length = cur_length
            
            # this is important in case the keyword is not yet appeared before
            padded_max_length = max(padded_max_length, max_length)          
            
            # identify complemented bogus count for each keyword 
            # and add them to the keyword's entry.
            for keyword, value in cached_data.items():
                bogus_count = 0
                if keyword in self.keywords_tracking:
                    bogus_count = padded_max_length - self.keywords_tracking[keyword] - len(value)
                else:
                    bogus_count = padded_max_length - len(value)
                
                # update throughput_num and padding_num
                batch_throughput_num += len(value)
                batch_padding_num += bogus_count
                        
                if bogus_count > 0:
                    bogus_ids = copy.deepcopy(self.padding_ds[keyword][0:bogus_count])    
                    if len(bogus_ids) < bogus_count:
                        print("Insufficient padding - %s - cached %i - expected bogus %i" % 
                              (keyword, len(value), len(bogus_ids)))
                        # self.stop()
                    else:
                        # update padding if successful
                        value.update(bogus_ids)
                        # remove them from padding_ds
                        for bogus_id in bogus_ids:
                            self.padding_ds[keyword].remove(bogus_id)
                        
                # update self.keywords_tracking
                if keyword in self.keywords_tracking:
                    self.keywords_tracking[keyword] += len(value)
                else:
                    self.keywords_tracking[keyword] = len(value)
  
            # return variables are
            return cached_data, dict(), batch_throughput_num, batch_padding_num
        
        else:  # this is for low-padding mode
  
            # this dict contains minimum throughput after padding 
            cur_min_cluster = dict()
            cur_min_cluster.clear()
            
            # falling back            
            falling_back_data = dict()  # {'a':{1,2,3},'f':{9,10,11}}
            
            # extract the min num to pad
            min_length = sys.maxsize
            for _, value in cached_data.items():
                if min_length > len(value) and len(value) != 0:
                    min_length = len(value)

            for keyword, value in cached_data.items():
                if len(value) > 0:
                    extracted_ids = set(random.sample(value, min_length))
                    cur_min_cluster[keyword] = extracted_ids
                    
                    temp = value - extracted_ids
                    if len(temp) > 0:
                        falling_back_data[keyword] = temp
                
                else:  # in case for adding keywords for flushing persistent
                    cur_min_cluster[keyword] = set([])
                    
            # double check against EDB
            padded_max_length = 0   
            for keyword in cur_min_cluster:
                if keyword in self.keywords_tracking:
                    padded_length = self.keywords_tracking[keyword] + min_length
                    if padded_length > padded_max_length:
                        padded_max_length = padded_length
            
            # cross-checking in case min_length is resulted by a newly added keyword
            # but it is not necessary, is optional for consistency
            padded_max_length = max(padded_max_length, min_length)
            
            # identify complemented bogus count for each keyword 
            # and add them to the keyword's entry.
            for keyword, value in cur_min_cluster.items():
                bogus_count = 0  # considering padding_count is the number of padding
                if keyword in self.keywords_tracking:
                    bogus_count = padded_max_length - self.keywords_tracking[keyword] - len(value)
                else:
                    bogus_count = padded_max_length - len(value)
                
                # update throughput_num and padding_num
                batch_throughput_num += len(value)
                batch_padding_num += bogus_count               
                
                if bogus_count > 0:
                    bogus_ids = copy.deepcopy(self.padding_ds[keyword][0:bogus_count])    
                    if len(bogus_ids) < bogus_count:
                        print("Insufficient padding - %s - cached %i - expected bogus %i" % 
                              (keyword, len(value), len(bogus_ids)))
                        #self.stop()
                    else:
                        value.update(bogus_ids)
                        
                        # remove them from padding_ds
                        for bogus_id in bogus_ids:
                            self.padding_ds[keyword].remove(bogus_id) 
                            
                # update self.keywords_tracking
                if keyword in self.keywords_tracking:
                    self.keywords_tracking[keyword] += len(value)
                else:
                    self.keywords_tracking[keyword] = len(value)
            
            # return variables are
            return cur_min_cluster, falling_back_data, batch_throughput_num, batch_padding_num                                           
