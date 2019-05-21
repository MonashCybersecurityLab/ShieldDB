import os, pickle
import json
import numpy as np

def retrieve_keywords_from_file(dst_dir, fileId):
    
    fp = open(os.path.join(dst_dir, fileId))
    data = fp.read()
    keywords = data.split(",") #these files have been stemmed 
    
    pairs = []
    for keyword in keywords:
        pairs.append((keyword,fileId))
    
    return pairs

def resetFolders(list_folders):
    
    for temp_folder in list_folders:
        if not os.path.exists(temp_folder):
            os.makedirs(temp_folder)
        else:
            filelist = [ f for f in os.listdir(temp_folder)]
            
            for f in filelist:
                os.remove(os.path.join(temp_folder, f))


def get_size(start_path = '.'):
    total_size = 0
    for dirpath, _, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

def find_cooccurence(keyword1,keyword2,lookUp):
    docID_keyword1 =  lookUp[keyword1]
    docID_keyword2 =  lookUp[keyword2]  
    return len(docID_keyword1 & docID_keyword2)


def check_known_query_index(query_index,query_map):
    known_queries = [item[0] for item in query_map]  
    return query_index in known_queries
   

def find_false_occurance_padding(lookUp,padded_lookUp):
    
    max_false_occurance = 0

    for key1, value1 in lookUp.items():
        for key2, value2 in lookUp.items():                      
                original_co_occurance = (len(value1 & value2))
                
                padded_docID_keyword1 =  padded_lookUp[key1] 
                padded_docID_keyword2 =  padded_lookUp[key2]
                
                padd_co_occurance = (len(padded_docID_keyword1 & padded_docID_keyword2)) 
                
                if max_false_occurance < abs(padd_co_occurance-original_co_occurance) :
                    max_false_occurance =  abs(padd_co_occurance-original_co_occurance)
                                                         
    return max_false_occurance

def check_consistency_with_unknown_query(candidate_keyword,Cq_co_value,possible_keyword_list_next_query,lookUp,max_false_occurance):
    
    isConsistency = False
    for next_query_candidate in possible_keyword_list_next_query:
        if candidate_keyword !=next_query_candidate:
            count_in_Cl = find_cooccurence(candidate_keyword,next_query_candidate,lookUp)      
            if (Cq_co_value >=  count_in_Cl) and (Cq_co_value <=  count_in_Cl + max_false_occurance):
                isConsistency = True
    
    return isConsistency

def check_known_query_keyword(candidate_keyword,query_map):
    known_queries = [item[2] for item in query_map]  
    return candidate_keyword in known_queries
    
def monitor_len_dataset(data_padding_set):
    size = 0
    for _,value in data_padding_set.items():
        size += len(value)
    
    return size

def count_key_value_pairs_clusters(current_clusters_fileIDs):
    #current_clusters_fileIDs is an array of dicts
    
    total_size = 0
    for index in range(len(current_clusters_fileIDs)):
        current = monitor_len_dataset(current_clusters_fileIDs[index])
        total_size += current
    
    return total_size

def dump_data_to_file(data,key_folder, filename):
    f = open(os.path.join(key_folder, filename),"wb")
    pickle.dump(data,f)
    f.close()

def load_data_from_file(key_folder, filename):
    f1 = open(os.path.join(key_folder, filename),"rb") 
    data = pickle.load(f1)
    f1.close()
    return data

def monitor_dict_size(keywords_tracking):
    count = 0
    for _,v in keywords_tracking.items():
        count +=v
    
    return count

def removeDocuments(lookUp,cache_client_storage):
    
    for cluster in cache_client_storage:
        for key,value in cluster.items():
            if key in lookUp:
                lookUp[key] = lookUp[key].difference(value)
    
    return lookUp

def identify_cur_padding_dataset(data_padding_set,paddedLookUp):
    #this function identifies the current padding data set based on the left
    
    for key,value in paddedLookUp.items():  
        bogus_doc =  [bogus_id for bogus_id in value if int(bogus_id) >= 100000]
        data_padding_set[key] = np.setdiff1d(data_padding_set[key], bogus_doc).tolist()
    
    return data_padding_set
        
def split_real_documents(raw_IDs, id_lim):
    real_set = set([])
    bogus_set = list([])
    for item in raw_IDs:
        if int(item) >= id_lim:
                bogus_set.append(item)
        else:
            real_set.add(item)
    
    return (real_set,bogus_set) 

def write_append(file_name, result):
    f = open(file_name,"w")
    json.dump(result, f)
    f.close() 