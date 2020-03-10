# Kmeans with triangle inequality on Apache Flink
This code is an extended version of the K-Means clustering algorithm provided as an example with Aapche Flink. The extenstion includes support for n-dimensional data, convergence stoppage and the use of triangle inequality to reduce the number of distance computations.

### Data Structure of unionData
unionData is a Tuple5 as shown below.

``` Tuple5<Integer, Integer, Centroid, Point, COI> ```

**Description of tuple fields**
| f0        	  | f1              | f2  			  |	f3			    |	f4			  |
|:---------------|:---------------|:---------------|:---------------|:---------------|
| Key <br>0: Centroid<br>1: Point<br>2: COI  		| ID of cluster Point is assigned to	| Centroid / null 	| Point / null	| COI / null	|







