import grpc
import math
import threading
from concurrent import futures
import numstore_pb2, numstore_pb2_grpc #these files will change

server_dict = {}
total = 0
#this cache saves the 10 largest numbers to have factorials calculated
cache = {}

class NumStore(numstore_pb2_grpc.NumStoreServicer):
    def SetNum(self, request, context):
        lock = threading.Lock()
        with lock:
            global server_dict
            global total
            key = request.key
            value = request.value
            if key in server_dict:
                old_value = server_dict[key]
                server_dict[key] = value
                total -= old_value
                total += value
            else:
                server_dict[key] = value
                total += value
            return numstore_pb2.SetNumResp(total=total)

    def Fact(self, request, context):
        lock = threading.Lock()
        with lock:
            global server_dict
            global cache
            key = request.key
            if key in server_dict:
                value = server_dict[key]
                cacherCode = cacher(value)
            else:
                return numstore_pb2.SetFactResp(error="error: key not in server_dict")
        #lock ends        
        if cacherCode[1] == True:
            return numstore_pb2.SetFactResp(value=cacherCode[0], hit=cacherCode[1])
        else:
            result = math.factorial(value)
            with lock:
                if cacherCode[0] == -2:
                    cache[value] = result
                    return numstore_pb2.SetFactResp(value=cache[value], hit=cacherCode[1])
                elif cacherCode[0] == -3:
                    cache.pop(min(cache))
                    cache[value] = result
                    return numstore_pb2.SetFactResp(value=cache[value], hit=cacherCode[1])
                else:
                    return numstore_pb2.SetFactResp(value=result, hit=cacherCode[1])

#this cache saves the 10 largest numbers to have factorials calculated
def cacher(value):
    global cache
    if value in cache:
        return [cache[value], True]
    elif len(cache) <= 10:
        #cache[value] = math.factorial(value)
        return [-2, False]
    elif value > min(cache):
        #cache.pop(min(cache))
        #cache[value] = math.factorial(value)
        return [-3, False]
    else:
        #math.factorial(value)
        return [-4, False]
        

def server():
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=[("grpc.so_reuseport", 0)])
  numstore_pb2_grpc.add_NumStoreServicer_to_server(
      NumStore(), server)
  print("Start listening on port 5440")
  #server.add_insecure_port('localhost:5440')
  server.add_insecure_port('[::]:5440')
  server.start()
  server.wait_for_termination()

server()
