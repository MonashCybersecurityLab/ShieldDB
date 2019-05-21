import os
import operator
import pickle
import csv

class App_Controller:
    def __init__(self):

        self.k_s = os.urandom(16) #key to encrypt the keyword
        self.k_d = os.urandom(16) #key to encrypt the fileId
        self.iv = os.urandom(16)
    
    def get_keys(self):
        return (self.k_s, self.k_d, self.iv)
    
    def check_valid_sentence(self, sentence):
        stopping_words_sentences = ["Message-ID:","Date:","From:","To:","Subject:","Cc:","Bcc:","X-From:","X-To:","X-cc:","X-bcc:","X-Folder:","X-Origin:","X-FileName:", 
                   "Mime-Version:","Content-Type:","charset=us","Content-Transfer-Encoding"]

        for word in stopping_words_sentences:
            if word in sentence:
                return False
        return True    

    def dump_frequency_file(self, trimed_sorted_dict_frequency, file_writer):
    
        #columnTitleRow = "keyword, frequency\n"
        #file_writer.write(columnTitleRow)
    
        for index in range(len(trimed_sorted_dict_frequency)):
            keyword = trimed_sorted_dict_frequency[index][0]
            frequency = trimed_sorted_dict_frequency[index][1]
            row = keyword + "," + str(frequency) + "\n"
            file_writer.write(row)    

    #clusters padding
    def clustering_parser(self,
                          keyword_num = 5000, 
                          alpha= 256,
                          distribution_dir='adding_distribution',
                          clusters_points_set = 'cluster_points_5000.csv',
                          clusters_dist = 'cluster_dist_5000.csv',
                          alpha_data_set = [256,512,768,1024]):
                                                         
        clusters_points = []

        #parse cluster_checking_points
        analysis_file = open(os.path.join(distribution_dir,clusters_points_set), "r", newline='')
        reader = csv.reader(analysis_file,delimiter=',')
    
        for row in reader:
            n_keywords = int(row[0])
            temp_alpha = int(row[1])
            if n_keywords == keyword_num and temp_alpha== alpha:
                clusters_points = eval(row[2])
                #append the last record
                clusters_points.append(keyword_num)
        
        print("Number of clusters: " + str(len(clusters_points)))
        
        clusters_keywords_props = self.read_padding_distribution(distribution_dir, 
                                       clusters_dist, 
                                       alpha_data_set.index(alpha),
                                       clusters_points)
        #format [[('a',0.23),('b',0.2),...], [], [], [], []  ]
        return clusters_keywords_props
    
    def read_padding_distribution(self, 
                                  data_dir,
                                  str_file_name,
                                  column,
                                  clusters_points):
    
        analysis_file = open(os.path.join(data_dir,str_file_name), "r", newline='')
        reader = csv.reader(analysis_file,delimiter=',')
        
        no_clusters = len(clusters_points)
        clusters_keywords = [[] for _ in range(no_clusters)]
        #format [[('a',0.23),('b',0.2),...],[],[],[],[]]
         
        cluster_counter = 0
        prob_column = column+2        
        
        for i, row in enumerate(reader): #i starts from 0
            keyword = row[0]
            probability = float(row[prob_column])
                
            if i+1 <= clusters_points[cluster_counter]:
                clusters_keywords[cluster_counter].append((keyword,probability))
            if i+1 == clusters_points[cluster_counter]:
                cluster_counter +=1

        return clusters_keywords

    #function samples streaming data set by using the constructed and inverted index
    #input : src inverted index file
    #output: streaming folder    
    def streaming_data_sampling(self, src_dir="padding_distribution",inverted_index="inverted_index_5000", streaming_dir="streaming"):
         
        f1 = open(os.path.join(src_dir, inverted_index), "rb") 
        inverted_dict = pickle.load(f1)
        f1.close()
        file_ids = set([])
        counter = 0 
        for keyword,id_list in inverted_dict.items():
            file_ids |=id_list
            counter +=1
            
        print("total keyword " + str(counter))
        print("total distinct file ids" + str(len(file_ids)))   

        file_counter = 1
        for fileid in file_ids:
            keyword_set = list([])
            for keyword,id_list in inverted_dict.items():
                if fileid in id_list:
                    keyword_set.append(keyword)  
            
            line = ",".join(keyword_set)
            #write the file and keyword_set to the file
            with open(os.path.join(streaming_dir, str(file_counter)), "a+") as myfile:
                myfile.write(line)
            
            file_counter +=1 
            print(str(file_counter))
