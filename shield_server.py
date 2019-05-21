from edb_controller import SSE_Server
from flask import Flask, abort, request, jsonify, make_response
import json, ast

app = Flask(__name__)

@app.errorhandler(400)
def not_found400(error):
    return make_response(jsonify( { 'error': 'Bad request' } ), 400)

@app.errorhandler(404)
def not_found404(error):
    return make_response(jsonify( { 'error': 'Resource is not found' } ), 404)

@app.route("/streaming", methods = ['POST'])
def streaming():

    data = request.get_json(force=True)
    if(len(data)==0) or 'enc_batch' not in data or server.getStatus() == False:
        abort(404)
    
    enc_batch = data['enc_batch']
    enc_batch = ast.literal_eval(enc_batch)
    result = server.import_encrypted_batch(enc_batch) 

    return json.dumps({'received size' : result})

@app.route("/search", methods = ['GET','POST'])
def search():

    result = request.get_json(force=True) #ignore the mimetype
    if(len(result)==0) or 'token' not in result or server.getStatus() == False:
        abort(404)    
    search_token = result['token']
    search_token = ast.literal_eval(search_token)
    
    if(len(search_token)) !=5:
        abort(404)

    encrypted_IDs  = server.search(search_token)

    return json.dumps({'result':str(encrypted_IDs)})


@app.route("/searchdel", methods = ['GET','POST'])
def searchdel():

    result = request.get_json(force=True) #ignore the mimetype
    if(len(result)==0) or 'token' not in result or server.getStatus() == False:
        abort(404)    
    search_token = result['token']
    search_token = ast.literal_eval(search_token)
    
    if(len(search_token)) !=5:
        abort(404)

    encrypted_IDs  = server.search_delete(search_token)

    return json.dumps({'result':str(encrypted_IDs)})


@app.route("/track/<int:value1>/<int:value2>", methods = ['PUT', 'GET'])
def track(value1,value2): 
    result = server.start_monitor(value1,value2)
    return json.dumps({'enable': result})

@app.route("/connect/<int:value>", methods = ['PUT', 'GET'])
def connect(value): 
    #1 --opem DB connection, #0 -- close DB connection, 
    #3 --disable threading monitor
    
    if (value==1):
        checkedOpen = server.open_conn()
        return json.dumps({'open': checkedOpen})
    if (value==0):
        checkedClose= server.close_conn()   
        return json.dumps({'close': checkedClose})  
    if (value==3):
        result = server.close_monitor()   
        return json.dumps({'disable': result})  
    else:
        abort(400)
         
@app.route("/dbinfo", methods = ['GET'])
def getInfo():
    if server.getStatus() == True: 
        info = server.get_db_info()
        return json.dumps({'info': info})
    else:
        abort(404)

#another rest service Deletion 

try:   
    if __name__ == '__main__':
    
        server = SSE_Server()
        print("Server is running ...")
        
        app.run(host='127.0.0.1',port=5000, debug=True)#True)
        
except KeyboardInterrupt:
    print("Exiting")
    exit(0)
