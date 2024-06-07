import sys
import os
import json
from math import sqrt
import grpc
from concurrent import futures
import random
import time
import comm_pb2
import comm_pb2_grpc


def serve(mapper_index):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapper_service = MapperService(mapper_index)  # Initialize the service with the mapper index
    comm_pb2_grpc.add_MapperServiceServicer_to_server(mapper_service, server)
    server.add_insecure_port(f'[::]:{50051 + int(mapper_index)}')  # Dynamic port assignment based on mapper index
    print(f"Mapper {mapper_index} running on port {50051 + int(mapper_index)}")
    server.start()
    server.wait_for_termination()


def euclidean_distance(point1, point2):
    return sqrt(sum((p1 - p2) ** 2 for p1, p2 in zip(point1, point2)))

def find_nearest_centroid(data_point, centroids):
    distances = [euclidean_distance(data_point, centroid) for centroid in centroids]
    nearest_centroid_index = distances.index(min(distances))
    return nearest_centroid_index

def map_function(data_points, centroids):
    map_output = []
    for data_point in data_points:
        nearest_centroid_index = find_nearest_centroid(data_point, centroids)
        map_output.append((nearest_centroid_index, data_point))
    return map_output

def partition(map_output, R):
    partitions = [[] for _ in range(R)]

    for key, value in map_output:
        partition_number = key % R
        partitions[partition_number].append((key, value))
    return partitions

def create_directory_for_mapper(mapper_index, R):
    mapper_main_dir = "Mappers"
    if not os.path.exists(mapper_main_dir):
        os.makedirs(mapper_main_dir)

    # Create a subdirectory for this specific mapper inside the 'Mappers' directory
    mapper_dir = os.path.join(mapper_main_dir, f"M{mapper_index}")
    if not os.path.exists(mapper_dir):
        os.makedirs(mapper_dir)

    dump_file_path = os.path.join(mapper_dir, "dump.txt")
    open(dump_file_path, 'a').close()

    for r in range(1, R+1):
        partition_file_path = os.path.join(mapper_dir, f"partition_{r}.txt")
        # Just create the file if it does not exist
        open(partition_file_path, 'a').close()
    return mapper_dir

def write_partitions_to_files(mapper_dir, partitions):

    for i, partition in enumerate(partitions, start=1):
        partition_file_path = os.path.join(mapper_dir, f"partition_{i}.txt")
        with open(partition_file_path, 'w') as file:
            for key_value in partition:
                # Convert the tuple to a JSON string
                file.write(json.dumps(key_value) + '\n')

def write_to_dump(message):
    print(message)
    with open(f"Mappers/M{mapper_index}/dump.txt", 'a') as file:
        file.write(message + "\n")


class MapperService(comm_pb2_grpc.MapperServiceServicer):
    def __init__(self, mapper_index):
        self.mapper_index = mapper_index

    def SendInput(self, request, context):

        # time.sleep(10) 
        try:
            # # Simulate a probabilistic failure
            # failure_probability = 0.5  # Set this to the desired probability of failure
            # is_successful = random.random() >= failure_probability
            is_successful = True

            if is_successful:
                file_path = request.input_file
                num_reducers = request.num_reducers
                mapper_index = request.mapper_index
                iteration_num = request.iteration_number
                centroids = [(c.x, c.y) for c in request.centroids]
                

                mapper_dir = create_directory_for_mapper(mapper_index, num_reducers)
                message = f"Mapper started the execution for iteration {iteration_num}."
                write_to_dump(message)

                
                # Read data points from the input .txt file provided in the request
                data_points = []
                with open(file_path, 'r') as file:
                    for line in file:
                        data_point = tuple(map(float, line.strip().split(',')))
                        data_points.append(data_point)
                
                map_output = map_function(data_points, centroids)
                
                partitions = partition(map_output, num_reducers)
      
                write_partitions_to_files(mapper_dir, partitions)
                write_to_dump("Mapper execution successful.")
                return comm_pb2.MapperResponse(success=True, message="Mapper execution successful.")
            else:
                write_to_dump("Error: Simulated mapper failure!!!")
                return comm_pb2.MapperResponse(success=False, message="Simulated mapper failure.")

        except Exception as e:
            return comm_pb2.MapperResponse(success=False, message=str(e))
        

    def GetPartitionedData(self, request, context):
        try:
            # mapper_index = request.mapper_index
            reducer_index = request.reducer_index
            partition_file_path = f"./Mappers/M{mapper_index}/partition_{reducer_index}.txt"

            # Read the partition data for the reducer
            partition_data = []
            with open(partition_file_path, 'r') as file:
                for line in file:
                    # Assuming each line in the file is a JSON-encoded key-value pair
                    partition_data.append(json.loads(line))

            # [[0, [1,3]], [0, [2,3]]]

            # partition_response = comm_pb2.PartitionResponse(success=True)
            # print(partition_data)

            simplified_data = []
            for entry in partition_data:
                simplified_entry = {
                    'id': entry[0],
                    'values': entry[1]
                }
                simplified_data.append(simplified_entry)
            print(simplified_data)
           
            return comm_pb2.PartitionResponse(success=True, entries = simplified_data)

            # return comm_pb2.PartitionResponse(entries=partition_data, success=True)

        except FileNotFoundError:
            # If the partition file doesn't exist, return an error
            msg = "Partition file not found.\n"
            write_to_dump()
            return comm_pb2.PartitionResponse(success=False, message="Partition file not found.")
        except Exception as e:
            # For any other exception, return an error with the exception message
            return comm_pb2.PartitionResponse(success=False, message=str(e))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python mapper.py <mapper_index>")
        sys.exit(1)
    
    mapper_index = sys.argv[1]  
    serve(mapper_index) 

