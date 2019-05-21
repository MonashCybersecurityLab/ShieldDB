import random, utilities
from sse_client import SSE_Client
from service_communicator import Service_Contactor
from timeit import default_timer as timer
import multiprocessing

class Search_Operator(multiprocessing.Process):
    def __init__(self, client_state_folder, 
                 edb_host='127.0.0.1', edb_port='5000'):
       
        super(Search_Operator, self).__init__()
        
        #read client state
        client_state = utilities.load_data_from_file(client_state_folder, "client")
        #read trueDB data to generate random query keywords
        self.keywords_tracking = utilities.load_data_from_file(client_state_folder, "keywords_tracking")
    
        self.cached_data_clusters = utilities.load_data_from_file(client_state_folder, "cache")
            
        self.sse_client = SSE_Client()
        self.sse_client.importClientState(client_state)
        
        #open edb connection 
        self.service_connector = Service_Contactor(edb_host,edb_port)
        
        

    def run(self):
        
        try:
            self.service_connector.open_connection()
            n_query_number= int(0.1 * len(self.keywords_tracking))   
            print("No. keywords " + str(len(self.keywords_tracking)))
            #serving counting attack
            access_patterns = []
                        
            #total ids
            #total_ids = 0
                             
            #select keywords, #note search token to perform search later
            n_query_keywords = random.sample(self.keywords_tracking.keys(),n_query_number)
                
            query_time = 0
            for query_keyword in n_query_keywords:
                start_time = timer() # in seconds
                
                search_token= self.sse_client.generateToken(query_keyword)
                encrypted_IDs = self.service_connector.search_connect(search_token)
                if encrypted_IDs is not None:
                    raw_ids = self.sse_client.decryptIDs(encrypted_IDs)
                else:
                    print("Keyword " + query_keyword +" is not tracked")
                _ = self.search_local_cache(query_keyword)
                
                end_time = timer() 
                
                access_patterns.append(raw_ids)
                #total_ids += len(encrypted_IDs)
                
                query_time += (end_time - start_time)
                
            avg_cur_trial = (query_time/n_query_number)*1000
            
            print("Avg search time (ms) per keyword: " + str(avg_cur_trial))
            #print("Total ids " + str(total_ids))
            #write search data to file

            
            utilities.dump_data_to_file(n_query_keywords, "tmp", "query")
            utilities.dump_data_to_file(access_patterns,"tmp", "access")
        #except:
        #    print("Search exception")
        finally:
            self.service_connector.close_connection()
            
    def search_local_cache(self, keyword):
        for cluster in self.cached_data_clusters:
            if keyword in cluster:
                return cluster[keyword]
        return None
