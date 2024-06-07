
import grpc
import logging
import os
import json
import sys
from concurrent import futures

import comm_pb2
import comm_pb2_grpc

class ReducerService(comm_pb2_grpc.ReducerService):
    def __init__(self, reducer_id):
        self.reducer_id = reducer_id
        self.iteration_number = 0

    def ProcessReduce(self, request, context):
        logging.info("Reducing data")
        all_partitioned_data = [] 

        try:
            # Load intermediate key-value pairs from mapper
            self.reducer_id = request.reducer_index
            self.iteration_number = request.iteration_number

            for i in range(1, request.num_mappers + 1):
                # Corrected function call within the same class
                intermediate_data = self.get_partitioned_data_from_mapper(i, self.reducer_id)
                all_partitioned_data.extend(intermediate_data)

            # Group data by centroid ID
            grouped_data = self.group_data_by_centroid(all_partitioned_data)

            # Compute updated centroids
            updated_centroids = self.compute_updated_centroids(grouped_data)

            self.create_directory_for_reducer(reducer_id)
            # Write updated centroids to output file
            output_file = f"./Reducers/R{self.reducer_id}.txt"
            self.write_centroids_to_file(updated_centroids, output_file)
            
            # Write updated centroids to the common file
            common_output_file = "centroids_temp.txt"
            self.write_centroids_to_common_file(updated_centroids, common_output_file)

            # Send acknowledgment
            return comm_pb2.ReducerResponse(success = True, message="SUCCESS")
        
        except Exception as e:
            # Send failure response in case of errors
            return comm_pb2.ReducerResponse(success = False, message=str(e))
    
    def create_directory_for_reducer(reducer_index, R):
        reducer_main_dir = "Reducers"
        if not os.path.exists(reducer_main_dir):
            os.makedirs(reducer_main_dir)


        reducer_file_path = os.path.join(reducer_main_dir, f"R{reducer_index}.txt")
        # Just create the file if it does not exist
        open(reducer_file_path, 'a').close()
        return reducer_main_dir


    def get_partitioned_data_from_mapper(self, mapper_index, reducer_index):
        # Construct the address using the mapper index to calculate the port
        map_addr = f"[::]:{50051 + mapper_index}"
        print(f"Connecting to mapper at address: {map_addr}")
        print(f"Mapper index: {mapper_index}")
        print(f"Reducer index: {reducer_index}")
        
        try:
            # Establish a gRPC channel and create a stub
            with grpc.insecure_channel(map_addr) as channel:
                stub = comm_pb2_grpc.MapperServiceStub(channel)
                # Prepare the request with both mapper and reducer indexes
                request = comm_pb2.GetPartitionedDataRequest(mapper_index=mapper_index, reducer_index=reducer_index)
                # Attempt to make the gRPC call to retrieve partitioned data
                response = stub.GetPartitionedData(request)
                print(response.success)
                print(response.entries)

                if response.success:
                    partitioned_data = []
                    for pair in response.entries:
                        print(pair)
                        # Ensure that pair is a valid object
                        if hasattr(pair, 'id') and hasattr(pair, 'values'):
                            # Directly access the values as they are floats
                            data = (pair.id, pair.values)
                            partitioned_data.append(data)
                        else:
                            raise ValueError("Invalid structure in response.entries")

                    return partitioned_data
                else:
                    # If the response was not successful, raise an exception with the message
                    raise Exception(f"Reducer: Failed to retrieve partitioned data: {response.message}")
        except grpc.RpcError as rpc_error:
            # Handle any gRPC-related errors
            raise Exception(f"An error occurred while making the gRPC call: {rpc_error}")
        except Exception as e:
            # Handle other exceptions that could occur
            raise Exception(f"An unexpected error occurred: {e}")



    def group_data_by_centroid(self, intermediate_data):
        grouped_data = {}
        for key, value in intermediate_data:
            if key not in grouped_data:
                grouped_data[key] = []
            grouped_data[key].append(value)
        return grouped_data

    def compute_updated_centroids(self, grouped_data):
        updated_centroids = {}
        for centroid_id, data_points in grouped_data.items():
            # Compute mean of data points for each centroid
            mean_point = self.compute_mean_point(data_points)
            updated_centroids[centroid_id] = mean_point
        return updated_centroids

    def compute_mean_point(self, data_points):
        total_points = len(data_points)
        num_dimensions = len(data_points[0])  # Assuming all data points have same dimensions
        mean_point = [0] * num_dimensions
        for point in data_points:
            for i in range(num_dimensions):
                mean_point[i] += point[i]
        mean_point = [coord / total_points for coord in mean_point]
        return mean_point

    def write_centroids_to_file(self, centroids, output_file):
        with open(output_file, 'w') as f:
            for centroid_id, centroid in centroids.items():
                f.write(f"{centroid}\n")
                
    def write_centroids_to_common_file(self, centroids, common_output_file):
        with open(common_output_file, 'a') as f:
            for centroid_id, centroid in centroids.items():
                f.write(f"{centroid}\n")


def serve(reducer_id):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    reducer_service = ReducerService(reducer_id)
    comm_pb2_grpc.add_ReducerServiceServicer_to_server(reducer_service, server)
    server.add_insecure_port(f'[::]:{50061 + int(reducer_id)}')
    print(f"Reducer {reducer_id} running on port {50061 + int(reducer_id)}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python reducer.py <reducer_id>")
        sys.exit(1)
    reducer_id = int(sys.argv[1])  
    serve(reducer_id)

