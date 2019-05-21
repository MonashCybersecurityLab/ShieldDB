import ast
import requests

class Service_Contactor:
    
    def __init__(self, server_host='127.0.0.1', server_port='5000'):
        
        url_builder = 'http://' + server_host + ':' + server_port + '/'
        
        self._open_connection_url = url_builder + 'connect/1'
        self._close_connection_url = url_builder + 'connect/0'
        
        self._enable_edb_monitor_url = url_builder + 'track/'
        self._disable_edb_monitor_url = url_builder + 'connect/3'
        
        self._streaming_url = url_builder + 'streaming'
        self._searching_url = url_builder + 'search'
        self._search_del_url = url_builder + "searchdel"
        self._db_info_url = url_builder + 'dbinfo'

        
    def proc_response(self,response, **kwargs):
        #print(">> Response1 ", response.request.url)
        #print (response.content.decode())
        if response.status_code != 200:
            print (response.request.url)
            print (response.content)
    
    def enable_edb_monitor(self,monitoring_duration, time_interval):
        _ = requests.put(self._enable_edb_monitor_url + str(monitoring_duration) + "/" + str(time_interval))
           
    def disable_edb_monitor(self):
        _ = requests.put(self._disable_edb_monitor_url) 
            
    #open connection to Server
    def open_connection(self):
        _ = requests.put(self._open_connection_url)
        #print(resp.json())

    #close connection to Server
    def close_connection(self):
        _ = requests.put(self._close_connection_url)
        #print(resp.json())

    def streaming_connect(self, enc_batch):

        jenc_batch1 = {'enc_batch': str(enc_batch)}
        #print(jenc_batch1)
        response = requests.post(self._streaming_url, json=jenc_batch1)
        return response

    def search_connect(self, token):
        if token is None:
            return None
        jtoken = {'token': str(token)}
        resp = requests.post(self._searching_url, json=jtoken)
        result = resp.json()
        str_encrypted_IDs  = result['result']
        encrypted_IDs = ast.literal_eval(str_encrypted_IDs)
        #raw_IDs = client.decryptIDs(encrypted_IDs)
        #print(raw_IDs)
        return encrypted_IDs 

    def search_del_connect(self,token):
        jtoken = {'token': str(token)}
        resp = requests.post(self._search_del_url, json=jtoken)
        result = resp.json()
        str_encrypted_IDs  = result['result']
        encrypted_IDs = ast.literal_eval(str_encrypted_IDs)

        return encrypted_IDs 
            
    def getDBInfo(self):
        resp = requests.get(self._db_info_url)
        return resp.json()

