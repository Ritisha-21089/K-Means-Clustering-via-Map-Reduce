# Project: Implementing K-Means using MapReduce from Scratch

## Learning Resources

For this project, you will need to be familiar with:

- **K-Means clustering algorithm:** [K-means Clustering: Algorithm, Applications, Evaluation Methods, and Drawbacks](https://www.javatpoint.com/k-means-clustering-algorithm)
- **MapReduce Model**
- **K-Means algorithm using MapReduce Framework:** Section 3.1 of the [paper](https://www.researchgate.net/publication/266151623_K-Means_Clustering_Algorithm_and_its_Applications)

## Setup

The entire MapReduce framework will be deployed on one machine. Each mapper, reducer, and master will be a separate process, each with a distinct port number. Intermediate and output data will be stored in a separate file directory in the local file system.

- **Mapper:** Persists intermediate data
- **Reducer:** Persists final output data
- **gRPC:** Used for RPCs

## K-Means Algorithm

The K-means algorithm partitions a dataset into K clusters and proceeds as follows:

1. Randomly initialize K cluster centroids.
2. Assign each data point to the nearest cluster centroid.
3. Recompute the cluster centroids based on the mean of the data points assigned to each cluster.
4. Repeat steps 2 and 3 for a fixed number of iterations or until convergence.

Refer to Section 3.1 of the [paper](https://www.researchgate.net/publication/266151623_K-Means_Clustering_Algorithm_and_its_Applications) for how the K-Means clustering algorithm can be performed in a distributed manner.

## Implementation Details

### Components

#### Master

The master process is responsible for running and communicating with other components. It requires the following parameters:

- Number of mappers (M)
- Number of reducers (R)
- Number of centroids (K)
- Number of iterations for K-Means

#### Input Split (invoked by master)

The input data will be divided into smaller chunks that can be processed by multiple mappers. There are two scenarios for partitioning:

1. **Single File:** Each mapper reads the entire input file and processes the indices allocated by the master.
2. **Multiple Files:** Each mapper is responsible for a subset of files allocated by the master.

The master does not distribute the actual data to the mappers to avoid unnecessary network traffic.

#### Map (invoked by mapper)

Each mapper will apply the Map function to its input split to generate intermediate key-value pairs:

- **Inputs:**
  - Input split assigned by the master
  - List of Centroids from the previous iteration
- **Outputs:**
  - Key: Index of the nearest centroid
  - Value: Value of the data point itself

The output will be written to a file in the mapper’s directory.

#### Partition (invoked by mapper)

The output of the Map function needs to be partitioned into smaller partitions. Each mapper should have R file partitions. There will be M * R partitions in total.

#### Shuffle and Sort (invoked by reducer)

The reducer will sort the intermediate key-value pairs by key and group values belonging to the same key.

#### Reduce (invoked by reducer)

The reducer will process the intermediate key-value pairs to generate the final output:

- **Inputs:**
  - Key: Centroid id
  - Value: List of data points belonging to this centroid id
- **Outputs:**
  - Key: Centroid Id
  - Value: Updated Centroid

The output will be written to a file in the reducer’s directory.

#### Centroid Compilation (invoked by master)

The master will compile the final list of centroids from the reducer outputs and store them in a single file. This list will be used as input for the next iteration.

### Fault Tolerance

If a mapper or reducer fails, the master should re-run the task. The master should ensure the failed task gets reassigned to the same or different mapper/reducer.

### gRPC Communication

The gRPC communication between processes for each iteration:

- Master ⇔ Mapper
- Master ⇔ Reducer
- Reducer ⇔ Mapper
- Master ⇔ Reducer

### Sample Input and Output Files

```
Data/
├─ Input/
│  ├─ points.txt
├─ Mappers/
│  ├─ M1/
│  │  ├─ partition_1.txt
│  │  ├─ partition_2.txt
│  │  ├─ partition_R.txt
│  ├─ M2/ ...
│  ├─ M3/ ...
├─ Reducers/
│  ├─ R1.txt
│  ├─ R2.txt
├─ centroids.txt
```

### Print Statements

Log/print everything in a `dump.txt` file for easy debugging/monitoring. Display the following data for each iteration:

- Iteration number
- Execution of gRPC calls to Mappers or Reducers
- gRPC responses for each Mapper and Reducer function (SUCCESS/FAILURE)
- Centroids generated after each iteration
