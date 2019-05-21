import hashlib
from itertools import cycle
from RocksDBWrapper import RocksDBWrapper as RocksWrapper   
from Crypto.Cipher import AES
import utilities
from flush_server_observer import Flushing_SMonitor

class SSE_Server:
    def __init__(self, ):
        self.rockwrappers = RocksWrapper()
        #is connectionRocksDB alive
        self.isAlive = False
        
        #flushing_monitor
        self.f_mode = False
        
    def import_encrypted_batch(self, encrypted_batch):
        
        for k,v in encrypted_batch.items():    
            self.rockwrappers.put(k,v)
            
            if self.f_mode:
                self.monitor.tracker += 1 
                
        return len(encrypted_batch) 
    
    def start_monitor(self, f_period,f_interval):
        self.f_mode = True
        self.monitor = Flushing_SMonitor(f_period,f_interval)
        print(">Monitoring thread enabled")
        self.monitor.start()
        return 1
        
    def close_monitor(self):
        print(">Monitoring thread disabled")
        self.monitor._stop()
        return 1
                                  
    def open_conn(self):
        self.isAlive = True
        return self.rockwrappers.open()   
                                  
    def close_conn(self):
        self.isAlive = False
        return self.rockwrappers.close()
    
    def get_db_info(self):
        return self.rockwrappers.getInfo()
    
    def getStatus(self):
        return self.isAlive
           
    def search(self, token):
         
        encrypted_IDs = list([])
        try:
            k_w = token[0]
            k_id= token[1]
            latest_state= token[2]
            latest_count= token[3]
            iv= token[4]
                    
            
            
            while(latest_count !=0):
                count_in_batch = 0
                for count_in_batch in range(latest_count):
                    primitive_enc_state_count = self.pseudo_function_F(latest_state,str(count_in_batch),iv)
                    u = self.primitive_hash_h(primitive_enc_state_count + k_w)
                    
                    v = self.rockwrappers.get(u) # data type of v is bytes
                    
                    v_part = self.primitive_hash_h(self.pseudo_function_F(latest_state,str(count_in_batch),iv) + k_id) 

                    encoded_id = [ a ^ b for (a,b) in zip(v, cycle(v_part)) ]
                    
                    #client needs to decrypt encoded_id with proper k_id 
                    encrypted_IDs.append(bytes(encoded_id))
                
                #retrieve the previous k_e and count
                count_in_batch +=1
                primitive_enc_key_count = self.pseudo_function_F(latest_state,str(count_in_batch),iv)
                u_k =  self.primitive_hash_h( primitive_enc_key_count + k_w)
                v_k =  self.rockwrappers.get(u_k) # v_k has been bytes
                
                v_k_part = self.primitive_hash_h(primitive_enc_key_count + k_id)  #256 binary length
                K_e_c =  [ a ^ b for (a,b) in zip(v_k, cycle(v_k_part)) ] #  this content k_c and previous count
                k_e_dec = bytes(K_e_c)
                
                k_e = k_e_dec[:16]
                str_previous_counter = k_e_dec[16:].decode("ascii")
                
                #update previous count
                latest_count = int(str_previous_counter)
                #identify previous state
                latest_state = self.pseudo_inverse_permutation_P(k_e,latest_state,iv)
        except:
            print("Exception")
            pass
        finally:
            return encrypted_IDs
    
    def search_delete(self, token):
        
        #extract token
        k_w = token[0]
        k_id= token[1]
        latest_state= token[2]
        latest_count= token[3]
        iv= token[4]
        
        #deletion key in server
        del_key = list([])
               
        encrypted_IDs = list([])
        
        while(latest_count !=0):
            count_in_batch = 0
            for count_in_batch in range(latest_count):
                primitive_enc_state_count = self.pseudo_function_F(latest_state,str(count_in_batch),iv)
                u = self.primitive_hash_h(primitive_enc_state_count + k_w)
                del_key.append(u)
                v = self.rockwrappers.get(u) # data type of v is bytes
                
                v_part = self.primitive_hash_h(self.pseudo_function_F(latest_state,str(count_in_batch),iv) + k_id) 

                encoded_id = [ a ^ b for (a,b) in zip(v, cycle(v_part)) ]
                
                #client needs to decrypt encoded_id with proper k_id 
                encrypted_IDs.append(bytes(encoded_id))
            
            #retrieve the previous k_e and count
            count_in_batch +=1
            primitive_enc_key_count = self.pseudo_function_F(latest_state,str(count_in_batch),iv)
            u_k =  self.primitive_hash_h( primitive_enc_key_count + k_w)
            del_key.append(u_k)
            v_k =  self.rockwrappers.get(u_k) # v_k has been bytes
            
            v_k_part = self.primitive_hash_h(primitive_enc_key_count + k_id)  #256 binary length
            K_e_c =  [ a ^ b for (a,b) in zip(v_k, cycle(v_k_part)) ] #  this content k_c and previous count
            k_e_dec = bytes(K_e_c)
            
            k_e = k_e_dec[:16]
            str_previous_counter = k_e_dec[16:].decode("ascii")
            
            #update previous count
            latest_count = int(str_previous_counter)
            #identify previous state
            latest_state = self.pseudo_inverse_permutation_P(k_e,latest_state,iv)
        
        
        # delete these k/values in dict
        for item in del_key:
            self.rockwrappers.delete(item)
                
        return encrypted_IDs  
    ####################### primitive functions ##############      
    
    def _pad_str(self, s, bs=32):
        return s + (bs - len(s) % bs) * chr(bs - len(s) % bs)
    
    def _unpad_str(self,s):
            return s[:-ord(s[len(s)-1:])]
    
    def primitive_hash_h(self, msg):
        m= hashlib.sha256()
        m.update(msg)
        hash_msg = m.digest()
        return hash_msg
    
    def pseudo_permutation_P(self, key, raw, iv):
        cipher = AES.new(key,AES.MODE_CBC,iv) #raw must be multiple of 16
        return cipher.encrypt(raw)
    
    def pseudo_inverse_permutation_P(self, key, ctext,iv):
        cipher = AES.new(key,AES.MODE_CBC,iv)
        return cipher.decrypt(ctext)   
    
    def pseudo_function_F(self, key, raw, iv):
        raw = self._pad_str(raw)
        cipher = AES.new(key,AES.MODE_CBC,iv)
        return cipher.encrypt(raw)
    
    def pseudo_inverse_function_F(self, key, ctext, iv):
        cipher = AES.new(key,AES.MODE_CBC,iv)
        return self._unpad_str(cipher.decrypt(ctext))  
    
    #######################Counting attack ######################################33
        
    def perform_count_attacks(self, lookUp, query_tokens,access_patterns, query_keywords,max_pad):
            
        query_number = len(query_tokens)
        
        ''' construct co_occurance Cq'''
        occurence_C_q = [[] for _ in range(query_number)]
        possible_candidate_list_dict ={} #each element should be a set 
        #initialise possible_candidate_list_dict
        for i in range(query_number):
            possible_candidate_list_dict[i] = set([])
          
        query_map = []
            
        for i in range(query_number):
            for j in range(query_number):

                docID_keyword1 =  access_patterns[i]
                docID_keyword2 =  access_patterns[j]            
                
                occurence_C_q[i].append(len(docID_keyword1 & docID_keyword2))

        ''' find out the possible candidate for each query token based on max_pad window'''
        ''' different queries can have identical candidates - these possible candidates must be available at server side
            format would be possible_candidate_list_dict = {1: [11,12,13,14,15,16], 2: [9,12,14,15]}
        '''
        #print("max pad" + str(max_pad))
        #for i in range(query_number):
            #print(str(len(access_patterns[i])))
            #keyword = query_keywords[i]
            #if keyword in lookUp:
            #    print(str(len(lookUp[keyword])) + " " + str(len(access_patterns[i]) - max_pad))
        
        for k,v in lookUp.items():
            for i in range(query_number):
                result_length = len(access_patterns[i])
                temp_len = len(v)
                if temp_len in range(result_length-max_pad, result_length+1):
                    if i in possible_candidate_list_dict:
                        possible_candidate_list_dict[i].add(k) #possible candidate for query token #i
                    else:
                        possible_candidate_list_dict[i] = set([k])
 
        ''' double check to see whether there is any query that only has one candidate '''                
        for query_index in range(query_number):
            if len(possible_candidate_list_dict[query_index]) == 1:
                for keyword in possible_candidate_list_dict[query_index]:
                    break
                query_map.append((query_index, query_tokens[query_index],keyword,len(access_patterns[query_index])))                   
                del possible_candidate_list_dict[query_index]

        #double check with keywords in query map        
        guess_count = 0

        for item in query_map:
            guesskeyword = item[2]
            correctkeyword = query_keywords[item[0]]
            if guesskeyword == correctkeyword:
                guess_count += 1
 
        print("Knowledge query map size caused by unique length is %i " % guess_count) 
                        
        ''' run the guess for other unknown queries'''      
        for key, value in possible_candidate_list_dict.items():
  
            query_index = key
            possible_keyword_list = value
            
            remove_keyword_set = [] #index of items that should be deleted              
                  
            for candidate_keyword in possible_keyword_list:
         
                '''  check with known queries first ''' 
                inconsistency_stop = False
                   
                for known_query in query_map:
                    if inconsistency_stop== False:  
                        count_in_Cq = occurence_C_q[query_index][known_query[0]]
                        count_in_Cl = utilities.find_cooccurence(candidate_keyword,known_query[2],lookUp)
                        if  (count_in_Cq < count_in_Cl) or  (count_in_Cq > (count_in_Cl+max_pad)):
                            remove_keyword_set.append(candidate_keyword)
                            inconsistency_stop = True
                                         
                ''' check with unknown queries '''
                if  inconsistency_stop ==False:
                    ''' verify with the next unknown query's candidates'''    
                    for unknown_query_index in range(query_number):
                        if inconsistency_stop== False and unknown_query_index!=query_index:     
                            isknown = utilities.check_known_query_index(unknown_query_index,query_map)    
                            if isknown == False:              
                                possible_keyword_list_next_query = possible_candidate_list_dict[unknown_query_index]
                                #current value of Cq
                                Cq_co_value = occurence_C_q[query_index][unknown_query_index]
                                isconsistency_unknown_queries = utilities.check_consistency_with_unknown_query(candidate_keyword,Cq_co_value,possible_keyword_list_next_query,lookUp,max_pad)
                                
                                if isconsistency_unknown_queries ==False:
                                    remove_keyword_set.append(candidate_keyword)
                                    inconsistency_stop = True
                                                            
            remove_keyword_set = list(set(remove_keyword_set))
            for item in remove_keyword_set:
                possible_keyword_list.remove(item)
                            
            if  len(possible_keyword_list) == 1: 
                #print(">> One remained" )
                for keyword in possible_keyword_list:
                    break
                                  
                query_map.append((query_index, query_tokens[query_index], keyword,len(access_patterns[query_index])))   
                if keyword == query_keywords[query_index]:
                    #print(">>" + str(query_index+1) + " True" ) 
                    guess_count+=1                    
                #else:
                #    print(">>" + str(query_index+1) + " False" )  
            #else:
                #    print(">>" + str(query_index+1) + " False -not distinguishable" )  
        return guess_count/query_number