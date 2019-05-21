import random
import utilities
from sse_client import SSE_Client
from timeit import default_timer as timer
from service_communicator import Service_Contactor
import multiprocessing


class Rebuild_Operator(multiprocessing.Process):

    def __init__(self, rebuild_cluster_keywords, client_state_folder,
                 edb_host='127.0.0.1', edb_port='5000'):
       
        super(Rebuild_Operator, self).__init__()

        self.rebuild_keywords = rebuild_cluster_keywords
                
        # read client state
        client_state = utilities.load_data_from_file(client_state_folder, "client")
        # read local cache
        self.cached_data_clusters = utilities.load_data_from_file(client_state_folder, "cache")
        
        # read curreng padding data set
        self.padding_dataset = utilities.load_data_from_file(client_state_folder, "paddingset")
        
        # read keyword tracking 
        self.keywords_tracking = utilities.load_data_from_file(client_state_folder, "keywords_tracking")        
        
        # may read the query keywords again, this is an array of array
        #self.queried_keywords = utilities.load_data_from_file(client_state_folder, "query")
      
        self.queried_keywords = rebuild_cluster_keywords#utilities.load_data_from_file(client_state_folder, "query")
      
        self.sse_client = SSE_Client()
        self.sse_client.importClientState(client_state)
        
        # open edb connection 
        self.service_connector = Service_Contactor(edb_host, edb_port)
        
        

    def run(self):

        cur_cluster_real_ids = dict()  # this dictionary contains real keywords that we later pad them to transfer to server
        outsourced_key_count = 0
        padding_count = 0
        
        self.service_connector.open_connection()
        ######## test
        total_ids = 0
        temp_total_search_time = 0.0
        for queried_keyword in self.queried_keywords:
               
            start_temp_search_time= timer()                
            search_token = self.sse_client.generateToken(queried_keyword)
            if search_token is not None:
                encrypted_IDs  = self.service_connector.search_connect(search_token)
                # and time to decryptID as well
                _ = self.sse_client.decryptIDs(encrypted_IDs) # considered as access patterns.
                total_ids += len(encrypted_IDs)
                
            #FARE COMPARE: NOT INCLUDE LOCAL SEARCH 
            #consider the time it search in local cache
            #_ = self.search_local_cache(queried_keyword)             
            end_temp_search_time = timer()
            temp_total_search_time += (end_temp_search_time - start_temp_search_time)
            
        print(">>>GROUND TEST searching queried keywords (ms) " + str((temp_total_search_time/len(self.queried_keywords)) * 1000))
        print(">>>Total ids " + str(total_ids))
        
        self.service_connector.close_connection()
        
        ##########     ending test 
             
        print("> Phase 1: Query back and deletion at DB, add back to padding dataset")
        self.service_connector.open_connection()
        
        start_time = timer()

        for keyword in self.rebuild_keywords:
            search_token = self.sse_client.generateToken(keyword)
            if search_token is not None:
                encrypted_IDs = self.service_connector.search_del_connect(search_token)
                raw_ids = self.sse_client.decryptIDs(encrypted_IDs)
                
                # update calculate
                outsourced_key_count += 1   
                #**** we need to reset tracking as latter we also add them again with different numbers of. of real and Padding
                self.keywords_tracking[keyword] = 0
             
                # we delete in client states
                self.sse_client.deleteState(keyword)
                # filter real document
                (real_ids, bogus_ids) = utilities.split_real_documents(raw_ids, 100000)  
                
                padding_count += len(bogus_ids)
                if keyword in self.padding_dataset:
                    cur_padding = self.padding_dataset[keyword]
                    mergedlist = list(set(cur_padding + bogus_ids))
                    self.padding_dataset[keyword] = mergedlist
                                
                # we keep track raw_IDS for later re-encryption
                cur_cluster_real_ids[keyword] = real_ids                
            
        end_time = timer()
        
        self.service_connector.close_connection()
         
        #print("updated keyword state " + str(len(self.sse_client.getKeywordState())))
        
        # rebuild time in phase 1 includes 
        # * query time + delete in server and client state + filter and put back in padding data set + reset tracking
            
        print(">>> rebuild time (ms) phase 1st " + str((end_time - start_time) * 1000))
        print(">>> no. outsourced keys of the cluster " + str(outsourced_key_count))
        print(">>> no. of original keys of the cluster " + str(len(self.rebuild_keywords)))
        print(">>> no. real downloaded ids " + str(utilities.monitor_len_dataset(cur_cluster_real_ids)))
        print(">>> no. bogus downloaded ids " + str(padding_count))      
        
        # we -re test the query keywords
        
        self.service_connector.open_connection()
        
        
        #how often these keywords occur in the query test
        count_in_query = len(set(self.rebuild_keywords).intersection(set(self.queried_keywords)))
        
        total_ids = 0
        temp_total_search_time =0.0
        for queried_keyword in self.queried_keywords:
               
            start_temp_search_time= timer()                
            search_token = self.sse_client.generateToken(queried_keyword) 
            if search_token is not None:
                encrypted_IDs  = self.service_connector.search_connect(search_token)
                # and time to decryptID as well
                _ = self.sse_client.decryptIDs(encrypted_IDs) # considered as access patterns.
                total_ids += len(encrypted_IDs)
                
            #consider the time it search in local cache
            #_ = self.search_local_cache(queried_keyword)
            end_temp_search_time = timer()
            temp_total_search_time += (end_temp_search_time - start_temp_search_time)
        
        self.service_connector.close_connection()
                    
        print(">>> no. of queried keywords " + str(len(self.queried_keywords)))
        print(">>> rebuild keywords in queried set " + str(count_in_query))   
        print(">>> re-test searching queried keywords (ms) " + str((temp_total_search_time/len(self.queried_keywords)) * 1000))    
        print(">>> total ids " + str(total_ids))
        
        
        print("> Phase 2: Pad the downloaded keyword ids using high mode, and outsource encrypted data to server")
        
        
        padding_count = 0
        self.service_connector.open_connection()
        
        start_re_encrypt_time = timer()
        
        max_length = 0
        for _,value in cur_cluster_real_ids.items():                
            if max_length < len(value):
                max_length = len(value)
        
        #finalise the padding bogus for each keyword
        for keyword, value in cur_cluster_real_ids.items():
            bogus_count = max_length - len(value)
            padding_count +=bogus_count
            
            if bogus_count >0:
                #select random bogus and add to the value
                random.shuffle(self.padding_dataset[keyword])
                bogus_files = self.padding_dataset[keyword][0:bogus_count]
                
                #update the list
                value.update(bogus_files)
                #update the padding dataset
                self.padding_dataset[keyword] = self.padding_dataset[keyword][bogus_count:]
                
                #reset tracking
                self.keywords_tracking[keyword] = len(value) 
        
        
           
        #encrypt and transfer to server
        encrypted_batch = self.sse_client.streaming(cur_cluster_real_ids)
        #transfer to server
        self.service_connector.streaming_connect(encrypted_batch)
            
        end_re_encrypt_time= timer()
        
        self.service_connector.close_connection()
        
        
        print(">>> Re-encrypt time (ms) " + str((end_re_encrypt_time - start_re_encrypt_time) * 1000))
        print(">>> Bogus used " + str(padding_count))
        
        print("> Phase 3: Re-search queried keywords") 
        
        
        self.service_connector.open_connection()
        
        total_ids = 0
        temp_total_search_time =0.0
        for queried_keyword in self.queried_keywords:
               
            start_temp_search_time= timer()                
            search_token = self.sse_client.generateToken(queried_keyword)
            if search_token is not None:
                encrypted_IDs  = self.service_connector.search_connect(search_token)
                # and time to decryptID as well
                _ = self.sse_client.decryptIDs(encrypted_IDs) # considered as access patterns.
                total_ids += len(encrypted_IDs)
                
            #consider the time it search in local cache
            #_ = self.search_local_cache(queried_keyword)            
            end_temp_search_time = timer()
            temp_total_search_time += (end_temp_search_time - start_temp_search_time)
        print(">>>re-test searching queried keywords (ms) " + str((temp_total_search_time/len(self.queried_keywords)) * 1000))
        print(">>> total ids " + str(total_ids))  
        
        self.service_connector.close_connection()
        
        #over write self.keywords_tracking
        #over write self.padding dataset
        #over write client state
        utilities.dump_data_to_file(self.padding_dataset,"tmp", "paddingset")
        utilities.dump_data_to_file(self.sse_client.exportClientState(),"tmp", "client")
        utilities.dump_data_to_file(self.keywords_tracking, "tmp", "keywords_tracking")
        
    def search_local_cache(self, keyword):
        for cluster in self.cached_data_clusters:
            if keyword in cluster:
                return cluster[keyword]
        return None
    