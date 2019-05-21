import rocksdb
import gc
import traceback

class RocksDBWrapper(object):
    
    __db = None
    
    def open(self):
        if self.__db is None: 
            self.__db = rocksdb.DB("shield.db", rocksdb.Options(create_if_missing=True))
            return "created"
        
        return "existed"
    def close(self):
        try:
            if self.__db is not None:
                del self.__db
                gc.collect()
                return "deleted"
        except AttributeError:
            gc.collect()
            traceback.print_exc()
            return "exception"
            
    def put(self,key,value):
        if self.__db is not None:
            self.__db.put(key,value)
    
    def get(self,key):
        if self.__db is not None:
            return self.__db.get(key)
        else:
            return None
        
    def delete(self,key):
        if self.__db is not None:
            self.__db.delete(key) 
    
    def getInfo(self):
        if self.__db is not None:
            #https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
            message = self.__db.get_property(b"rocksdb.estimate-num-keys").decode()
            message += "\n"
            message += self.__db.get_property(b"rocksdb.stats").decode()
            return message
        else:
            return None