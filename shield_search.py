import time
from search_operation import Search_Operator

if __name__ == '__main__':
        
    client_folder = "tmp"
        
    search_test = Search_Operator(client_folder,'127.0.0.1' , '5000')
    search_test.start()
    
    search_test.join()
    search_test.terminate()
    
    time.sleep(15)