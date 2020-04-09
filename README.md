# Kmeans with triangle inequality on Apache Flink
This code is an extended version of the K-Means clustering algorithm provided as an example with Aapche Flink. The extenstion includes support for n-dimensional data, convergence stoppage and the use of triangle inequality to reduce the number of distance computations.

## Usage

### Arguments
Development environment (IntelliJ IDE):

```
KMeansTI
 	--d <n dimensions>
 	--k <n clusters>
 	--iterations <n iterations>
 	--points <path>
 	--centroids <path>
 	--output <path>
```

Production environment (Flink 1.9.1):

```
flink-1.9.1/bin/flink run -c com.ringdalen.kmeans.KMeans flink-kmeans-0.1.jar \
	--d 3 \
	--k 3 \
	--iterations <n iterations>
	--points file:///<path> \
	--centroids file:///<path> \
	--output file:///<path>
```


### Input
Input is a CSV file with " " (space) as separators, no header and only Int or Double.

Example CSV line for points: ``` <true_class> <feature_1> ... <feature_n>```

True class should be set to 0 if the class is not known in advance.

Example CSV line for centroids: ``` <id / class> <feature_1> ... <feature_n>```

ID / Class must be integers starting from 1 up to n.

### Output
Output is a CSV file with " " (space) as separators and, no header and only Int or Double.

Example CSV line:

``` <predicted_class> <true_class> <feature_1> ... <feature_n>```

## Implementation details
Documentation regarding the datastructures used within this program is outlined below.

### Data Structure of unionData
unionData is a Tuple5 as shown below.

``` Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI> ```

**Description of tuple fields**
| f0        	  | f1              | f2  			  |	f3			    |	f4			  |	f5			  |	f6			  |
|:---------------|:---------------|:---------------|:---------------|:---------------|:---------------|:---------------|
| Key <br>0: Centroid<br>1: Point<br>2: COI  		| ID of cluster Point is assigned to	| Point / null 	| Double / null (Upper bound)	| Double[K] / null (K lower bounds)	| Centroid / null	| COI / null	|







