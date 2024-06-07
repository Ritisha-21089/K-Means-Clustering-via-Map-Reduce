import random
import grpc
import comm_pb2
import comm_pb2_grpc
import json
from threading import Thread

data_points = []
cluster_centers = []
thread_list = []

def read_input_data(path):
    with open(path) as file:
        for line in file:
            x, y = line.split(",")
            data_points.append((float(x), float(y)))

import math


def dump_log(message):
    print(message)
    with open("dump_master.txt", "a") as file:
            json.dump(message, file)


def initialize_cluster_centers(num_clusters):
    """ Initialize cluster centers based on the first `num_clusters` data points. """
    global cluster_centers
    cluster_centers = [data_points[i] for i in range(num_clusters)]

def have_converged(centroids_from_file, current_centroids, tolerance=0.001):
    """ Check if centroids have converged within a certain tolerance. """
    if len(centroids_from_file) != len(current_centroids):
        return False  # Cannot be the same if their lengths are different
    
    for (x1, y1), (x2, y2) in zip(centroids_from_file, current_centroids):
        if math.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2) > tolerance:
            return False  # They differ more than the tolerance
    
    return True
    
    
def create_data_files_for_mappers(data_points, num_mappers):
    num_points_per_mapper = len(data_points) // num_mappers
    mapper_files = []

    for mapper_index in range(1, num_mappers + 1):
        start_index = (mapper_index - 1) * num_points_per_mapper
        if mapper_index == num_mappers:
            end_index = len(data_points)
        else:
            end_index = start_index + num_points_per_mapper

        filename = f"mapper_data_{mapper_index}.txt"
        mapper_files.append(filename)

        with open(filename, 'w') as file:
            for point in data_points[start_index:end_index]:
                file.write(f"{point[0]},{point[1]}\n")

    return mapper_files

def create_point_list(data_points):
    """ Converts a list of tuple data points into protobuf Point messages. """
    return [comm_pb2.Point(x=x, y=y) for (x, y) in data_points]


def notify_mapper(file_path, num_iteration, mapper_index, num_reducers):
    # Create the address string using the mapper index
    mapper_addr = f"localhost:{50051 + int(mapper_index)}"

    # Establish a gRPC channel
    with grpc.insecure_channel(mapper_addr) as channel:
        # Create a stub (client)
        stub = comm_pb2_grpc.MapperServiceStub(channel)
        
        protobuf_centroids = create_point_list(cluster_centers)

        # Prepare the request
        map_data = comm_pb2.MapperInput(
            input_file=file_path,
            num_reducers=num_reducers,
            mapper_index=mapper_index,
            iteration_number=num_iteration,
            centroids= protobuf_centroids
        )

        # Send the request to the mapper and receive the response
        try:
            response = stub.SendInput(map_data)
            if response.success:
                print("Mapper operation succeeded:", response.message)
                return True
            else:
                print("Mapper operation failed:", response.message)
                return False
        except grpc.RpcError as e:
            print(f"Failed to connect or call mapper service at {mapper_addr}: {e}")
            return False

def notify_parallel_mapper( file_path, num_reducers, num_mappers, mapper_index, iteration):
    sucess = False
    current_mapper_index = mapper_index

    while not sucess:
        print(f"Task is assigned to Mapper {current_mapper_index}, notifying via GRPC.")
        sucess = notify_mapper(file_path, iteration, current_mapper_index, num_reducers)
        if sucess:
            print(f"Mapper {current_mapper_index} completed the task assigned to it.")
        else:
            print(
                f"Mapper {current_mapper_index} failed to complete the task assigned to it."
            )
            current_mapper_index = (current_mapper_index % num_mappers) + 1



def notify_reducer(iteration_num, current_reducer_index, num_mappers):
    reducer_addr = f"localhost:{50061 + int(current_reducer_index)}"

    with grpc.insecure_channel(reducer_addr) as channel:
        stub = comm_pb2_grpc.ReducerServiceStub(channel)
        reduce_data = comm_pb2.ReducerInput(
            iteration_number=iteration_num,
            reducer_index=current_reducer_index,
            num_mappers=num_mappers,
        )

        try:
            response = stub.ProcessReduce(reduce_data)
            if response.success:
                print(f"Reducer {current_reducer_index} operation succeeded:", response.message)
                return True
            else:
                print(f"Reducer {current_reducer_index} operation failed:", response.message)
                return False
        except grpc.RpcError as e:
            print(f"Failed to connect or call reducer service at {reducer_addr}: {e}")
            return False




def notify_parallel_reducer(iteration, reducer_index, num_mappers, num_reducers):
    sucess = False
    current_reducer_index = reducer_index
    while not sucess:
        print(f"Task is assigned to Reducer {current_reducer_index}, notifying via GRPC.")
        sucess = notify_reducer(
            iteration, current_reducer_index, num_mappers
        )
        # sucess = sucessPair[1]
        if sucess:
            msg = (f"Reducer {current_reducer_index} completed the task assigned to it.")
            dump_log(msg)
            # thread_reducer_retry_list.append(sucessPair[0])
        else:
            msg =  f"Reducer {current_reducer_index} failed to complete the task assigned to it."
            dump_log(msg)
            current_reducer_index = (current_reducer_index % num_reducers) + 1


def join_and_clear_threads(thread_list):
    for thread in thread_list:
        thread.join()
    thread_list.clear()



def read_and_empty_file(file_path):
    centroids = []
    with open(file_path, 'r') as file:
        for line in file:
            # print(line)
            line = line.split(',')
            if len(line) == 2:  # Ensure there are exactly two parts (x, y)
                try:
                    x = float(line[0][1:])

                    y = float(line[1][:-3])

                    centroids.append((x, y))
                except ValueError:
                    print("Warning: Invalid data found and will be skipped -", line)
            else:
                print("Warning: Invalid data format -", line)
                
    # Empty the file after reading
    with open(file_path, 'w') as file:
        file.truncate(0)
        
    return centroids




if __name__ == "__main__":
    point_id = input("Enter the point id: ")
    input_path = f"Input/points{point_id}.txt"
    read_input_data(input_path)

    num_mappers = int(input("Enter the number of mappers: "))
    num_reducers = int(input("Enter the number of reducers: "))
    num_clusters = int(input("Enter the number of clusters: "))
    max_iterations = int(input("Enter the maximum number of iterations: "))

    initialize_cluster_centers(num_clusters)

    msg = f"Initial Cluster Centers are: {cluster_centers}. \n"
    dump_log(msg)
    
    mapper_files = create_data_files_for_mappers(data_points, num_mappers)

    for iteration in range(1, max_iterations + 1):
        print(f"Iteration {iteration}")

        for mapper_index in range(1, num_mappers + 1):
            file_path = mapper_files[mapper_index - 1]  # Get the filename for this mapper
            thread = Thread(
                target=notify_parallel_mapper,
                args=(
                    file_path,  # Pass the filename instead of start_index and end_index
                    num_reducers,
                    num_mappers,
                    mapper_index,
                    iteration,
                ),
            )
            thread_list.append(thread)
            thread.start()

        join_and_clear_threads(thread_list)

        run_again = False
        for reducer_index in range(1, num_reducers + 1):
            thread = Thread(
                target=notify_parallel_reducer,
                args=(iteration, reducer_index, num_mappers, num_reducers),
            )
            thread_list.append(thread)
            thread.start()

        join_and_clear_threads(thread_list)

        centroid_itr = read_and_empty_file("centroids_temp.txt")
        message = f"Centroids after iteration {iteration}:  {centroid_itr}. \n"    
        dump_log(message)

    
        # print(centroid_itr)
        if have_converged(centroid_itr, cluster_centers):
            msg = f"The algorithm has converged for cluster centers after iteration {iteration}. \n"
            dump_log(msg)
            
            break
        else:
            cluster_centers = centroid_itr


    msg = f"Final Cluster Centers are: {cluster_centers}. \n"
    dump_log(msg)

    with open("centroids.txt", "w") as file:
        for center in cluster_centers:
            file.write(f"{center[0]:.4f},{center[1]:.4f}\n")
