import grpc
import pandas as pd
import time
import threading
from concurrent import futures
import station_pb2, station_pb2_grpc #these files will change

from cassandra.cluster import Cluster
from cassandra.cluster import ConsistencyLevel
try:
    cluster = Cluster(['project-5-nateproject5-db-1', 'project-5-nateproject5-db-2', 'project-5-nateproject5-db-3'])
    session = cluster.connect()
except Exception as e:
    print(e)

class Station(station_pb2_grpc.StationServicer):
    def RecordTemps(self, request, context):
        global session
        insert_statement = session.prepare("""
            INSERT INTO weather.stations
            (id, date, record)
            VALUES
            (?, ?, {tmin: ?, tmax: ?})
            """)
        insert_statement.consistency_level = ConsistencyLevel.ONE
        try:
            session.execute(insert_statement, (request.station, request.date, request.tmin, request.tmax))
            return station_pb2.RecordTempsReply(error=None)
        except Exception as e:
            return station_pb2.RecordTempsReply(error=e)

    def StationMax(self, request, context):
        global session
        max_statement = session.prepare("""
            SELECT MAX(record.tmax)
            FROM weather.stations
            WHERE id=?
            """)
        max_statement.consistency_level = ConsistencyLevel.THREE
        try:
            tmax = pd.DataFrame(session.execute(max_statement, (request.station,)))["system_max_record_tmax"][0]
            return station_pb2.StationMaxReply(tmax=tmax, error=None)
        except Exception as e:
            return station_pb2.StationMaxReply(error=e)


def server():
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=[("grpc.so_reuseport", 0)])
  station_pb2_grpc.add_StationServicer_to_server(
      Station(), server)
  #server.add_insecure_port('localhost:5440')
  server.add_insecure_port('[::]:5440')
  print("Start listening on port 5440")
  server.start()
  server.wait_for_termination()

server()
