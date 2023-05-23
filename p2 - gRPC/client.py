import sys
import random
import threading
import time
import numpy
import grpc
import numstore_pb2, numstore_pb2_grpc

port = sys.argv[1]
addr = f"127.0.0.1:{port}"
channel = grpc.insecure_channel(addr)
stub = numstore_pb2_grpc.NumStoreStub(channel)

keys = []
hits = []
response_times = []
#processes = 0

#generate 100 possible keys
for i in range(100):
    key = "k" + str(i)
    keys.append(key)

def randRequest():
    global keys
    #global processes
    #processes+=1
    randNum = 0
    randNum = random.randint(0,10)
    if randNum <= 5:
        resp = stub.SetNum(numstore_pb2.SetNumRequest(key=keys[random.randint(0,99)], value=random.randint(1,15)))
    else:
        resp = stub.Fact(numstore_pb2.FactRequest(key=keys[random.randint(0,99)]))
        hits.append(resp.hit)
        #print("hit: " + str(resp.hit))
        #print("error: " + str(resp.error))

#create 8 server threads and send 100 random requests to the server from each thread
for i in range(100):
    t1 = threading.Thread(target=randRequest)
    t2 = threading.Thread(target=randRequest)
    t3 = threading.Thread(target=randRequest)
    t4 = threading.Thread(target=randRequest)
    t5 = threading.Thread(target=randRequest)
    t6 = threading.Thread(target=randRequest)
    t7 = threading.Thread(target=randRequest)
    t8 = threading.Thread(target=randRequest)
    start = time.time()
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t6.start()
    t7.start()
    t8.start()
    t1.join()
    t2.join()
    t3.join()
    t4.join()
    t5.join()
    t6.join()
    t7.join()
    t8.join()
    end = time.time()
    total = end-start
    response_times.append(total)

#calculate hit rate
true = 0
for i in range(len(hits)):
    if hits[i] == True:
        true+=1
hitRate = true / len(hits)

#print results
print("cache hit rate: " + str(hitRate))
print("p50 response time: " + str(numpy.percentile(response_times, [50])))
print("p99 response time: " + str(numpy.percentile(response_times, [99])))
#print()
#print("DEBUG")
#print("total hits: " + str(len(hits)))
#print("total response times: " + str(len(response_times)))
#print("total processes: " + str(processes))
