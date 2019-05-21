import random
import utilities
from app_controller import App_Controller

def generatePaddingData(clusters_keywords_props,*kwargs):
        #format [[('a',0.23),('b',0.2),...], [], [], [], []  ]

        max_bogus_size = kwargs[0]['max_bogus_size']
        file_count = [(100000 + i) for i in range(1,max_bogus_size+1)]
        padding_ds = {}
        
        count = 0
          
        for cluster in clusters_keywords_props:
            count +=1
            print("Processing cluster " + str(count) +"/" + str(len(clusters_keywords_props)))
            
            for keyword_prop in cluster:
                keyword = keyword_prop[0]
                probability = keyword_prop[1]
                
                bogus_file_no = int(probability * max_bogus_size * 1500) #800 is average document/file

                if bogus_file_no < max_bogus_size/2:
                    bogus_file_no = int(max_bogus_size/2)
                    
                random.shuffle(file_count)
            
                bogus_ids = file_count[0:bogus_file_no]
                padding_ds[keyword] = bogus_ids
            
 
              
        #dump to file
        if kwargs[0]['writable'] == True:
            utilities.dump_data_to_file(padding_ds, kwargs[0]['dir'],kwargs[0]['file_name'])
            

if __name__ == '__main__':

    alpha= 512#1024
    #perform identifying keywords in clusters with minimal padding
    keyword_num = 5000
    distribution_dir="padding_distribution"
    data_folder_dir = "data" + str(alpha)
    clusters_points_set = "cluster_points_5000.csv"
    clusters_dist = "cluster_dist_5000.csv"
    alpha_data_set = [256,512,768,1024]

    app = App_Controller()

    clusters_keywords_props = app.clustering_parser(keyword_num, 
                                                    alpha,
                                                    distribution_dir,
                                                    clusters_points_set,
                                                    clusters_dist,
                                                    alpha_data_set)
        
    max_bogus_size= 800000#550000 #300 keywords/file averagely
    write_info = {'writable':True,'dir':data_folder_dir,'file_name':"data_padding_set_1500",'max_bogus_size': max_bogus_size }
    generatePaddingData(clusters_keywords_props,write_info)    
