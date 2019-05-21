from rebuild_operation import Rebuild_Operator
import utilities
import time
if __name__ == '__main__':
    
    #should test with high padding mode to see the effectiveness
    
    #only for rebuild mode
    rebuild_mode = False
    alpha = 256
    
    client_state_folder = "tmp"
    
    #select cluster to rebuild and all their corresponding keywords
    key_folder = "data" + str(alpha)
    fixed_clusters_keywords = utilities.load_data_from_file(key_folder,"fixed_clusters_keywords")
    prob_clusters = utilities.load_data_from_file(key_folder,"prob_clusters")         
    
    largest_cluster_index = prob_clusters.index(max(prob_clusters))
    query_keywords = fixed_clusters_keywords[largest_cluster_index]
            
    rebuild_operator = Rebuild_Operator(query_keywords, client_state_folder)
    rebuild_operator.start()
    rebuild_operator.join()
    rebuild_operator.terminate()

    time.sleep(10)