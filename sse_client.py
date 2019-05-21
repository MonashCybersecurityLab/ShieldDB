import hashlib
from itertools import cycle
import os, copy
from Crypto.Cipher import AES

class SSE_Client:
    
    def __init__(self, keys= None):

        if keys == None:
            self.k_s = os.urandom(16) #key to encrypt the keyword
            self.k_d = os.urandom(16) #key to encrypt the fileId
            self.iv = os.urandom(16) #we share the same iv
        
        else:
            self.k_s = keys[0]
            self.k_d = keys[1]
            self.iv = keys[2]
                
        #initialize the map
        self.keyword_map = {} 
        #store  latest state and count of every keyword , like 'w':(1,2)
    
    def exportClientState(self):
        return (self.k_s, self.k_d, self.keyword_map, self.iv)

    def importClientState(self, data):
        self.k_s = data[0]
        self.k_d = data[1]
        self.keyword_map = data[2]
        self.iv = data[3]
                          
    def streaming(self, batch):
        
        encrypted_batch = dict()
               
        #we generate a new ephemeral key
        k_E = os.urandom(16)
        for keyword, ids in batch.items():
            if len(ids) > 0:
                #generate the token key and id_key
                primitive_hash_keyword = self.primitive_hash_h(bytes(keyword,'utf-8'))
                k_w = self.pseudo_permutation_P(self.k_s,primitive_hash_keyword,self.iv)
                k_id = self.pseudo_permutation_P(self.k_d,primitive_hash_keyword,self.iv)
                
                cur_state = None
                previous_count = None
                
                if keyword in self.keyword_map:
                    cur_state = self.keyword_map[keyword][0]
                    previous_count = self.keyword_map[keyword][1] 
                else:
                    cur_state =  os.urandom(16)
                    previous_count = 0                
                
                #generate a new state
                latest_state = self.pseudo_permutation_P(k_E,cur_state,self.iv)
                
                #update the latest state into the keyword_map
                self.keyword_map[keyword] = (latest_state,len(ids))
                
                counter = 0
                #encrypted each id with the latest state and count
                for fileId in ids:
                    
                    primitive_enc_state_count = self.pseudo_function_F(latest_state,str(counter),self.iv)
                    
                    u = self.primitive_hash_h(primitive_enc_state_count + k_w)
                    
                    v_part = self.primitive_hash_h(primitive_enc_state_count + k_id) 
                    #for internal extra permutation fileId
                    
                    encoded_id = self.pseudo_function_F(self.k_d,str(fileId),self.iv)
                    v = [ a ^ b for (a,b) in zip(encoded_id, cycle(v_part))] #cycle for the key
                    
                    encrypted_batch[u] = bytes(v)
                    
                    #increase the counter
                    counter +=1
                
                #we append a last epheramal key and counter to the end of this list
                primitive_enc_key_count = self.pseudo_function_F(latest_state,str(counter),self.iv)
                
                u_k =  self.primitive_hash_h( primitive_enc_key_count + k_w)
                v_k_part = self.primitive_hash_h(primitive_enc_key_count + k_id)  #256 binary length
                #embed epheral key and previous count
                previous_count_bytes =  bytes(str(previous_count), 'utf-8')
                v_k_part2 = k_E + previous_count_bytes
                v_k = [ a ^ b for (a,b) in zip(v_k_part2, cycle(v_k_part))]
                encrypted_batch[u_k] = bytes(v_k)
            
        return encrypted_batch

    def generateToken(self, keyword):
        
        #generate the token key and id_key
        primitive_hash_keyword = self.primitive_hash_h(bytes(keyword,'utf-8'))
        k_w = self.pseudo_permutation_P(self.k_s,primitive_hash_keyword,self.iv)
        k_id = self.pseudo_permutation_P(self.k_d,primitive_hash_keyword,self.iv)
         
        #retrieve latest state and count from the keyword
        if keyword in self.keyword_map:
            (latest_state, latest_count) = self.keyword_map[keyword]
            return (k_w, k_id, latest_state, latest_count, self.iv)
        else:
            return None
        
    def deleteState(self, keyword):
        if keyword in self.keyword_map:
            del self.keyword_map[keyword]
    
    def getKeywordState(self):
        return copy.deepcopy(self.keyword_map)
        
    def decryptIDs(self, encrypted_IDs):
        
        raw_ids = set([])
        
        for item in encrypted_IDs:
            cur_id = self.pseudo_inverse_function_F(self.k_d, item, self.iv)
            cur_id= cur_id.decode("utf-8")
            raw_ids.add(cur_id)
        
        return raw_ids
           
    def resetKeywordStateMap(self):
        if self.keyword_map !=None:        
            self.keyword_map.clear()
            self.keyword_map = None
            

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