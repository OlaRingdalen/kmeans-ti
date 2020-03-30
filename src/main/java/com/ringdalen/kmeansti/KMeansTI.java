package com.ringdalen.kmeansti;

import com.ringdalen.kmeansti.datatype.DataTypes.Centroid;
import com.ringdalen.kmeansti.datatype.DataTypes.Point;
import com.ringdalen.kmeansti.datatype.DataTypes.COI;

import com.ringdalen.kmeansti.util.Read;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * This code is an extended version of the K-Means clustering algorithm provided as an example with Aapche Flink.
 *
 * Usage: KMeansTI
 *          --points <path>
 *          --centroids <path>
 *          --output <path>
 *          --iterations <n iterations>
 *          --d <n dimensions>
 */

@SuppressWarnings("serial")
public class KMeansTI {

    public static void main(String[] args) throws Exception {

        // Fetching input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // Set up execution environment. getExecutionEnvironment will work both in a local IDE as well as in i
        // cluster infrastructure.
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // Read both points and centroids form files
        DataSet<Tuple4<Integer, Point, Double, Double[]>> points = Read.PointsFromFile(params, env);
        DataSet<Centroid> centroids = Read.CentroidsFromFile(params, env);

        // Fetching max number of iterations loop is executed with
        int iterations = params.getInt("iterations", 10);



        ///////////////////////// Initial mapping of points and computation of new centroids /////////////////////////

        // Computing the COI information
        DataSet<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> coiTuple = centroids
                .reduceGroup(new computeCOIInformation())
                .withBroadcastSet(centroids, "oldCentroids");

        DataSet<COI> coi = coiTuple.map(new ExtractCOI());

        // Select the initial cluster the point is assigned to
        DataSet<Tuple4<Integer, Point, Double, Double[]>> initialClusteredPoints = points
                .map(new SelectInitialNearestCenter())
                // Broadcast data that is needed in the initial clustering to each node
                .withBroadcastSet(centroids, "centroids")
                .withBroadcastSet(coi, "coi");

        // Producing new centroids based on the initial clustered points
        DataSet<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>>
                initialCentroidsTuple = initialClusteredPoints
                // Count and sum point coordinates for each centroid
                .map(new CountAppender()).groupBy(0).reduce(new CentroidAccumulator())
                // Compute new centroids from point counts and coordinate sums
                .map(new CentroidAverager());

        ///////////////////////// Initial mapping of points and computation of new centroids /////////////////////////



        // Expand the points into a Tuple7 in order to union it with the other DataSets
        DataSet<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>>
                initialPointsTuple = initialClusteredPoints.map(new ExpandPointsTuple());

        // Combine the points and centroids DataSets to one DataSet
        DataSet<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>>
                unionData = initialPointsTuple.union(initialCentroidsTuple.union(coiTuple));



        // ************************************************************************************************************
        //  Loop begins here
        // ************************************************************************************************************

        // Use unionData to iterate on for n iterations, as specified in the input arguments. This is the beginning
        // of the loop
        IterativeDataSet<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>>
                loop = unionData.iterate(iterations);

        // Separate points in a DataSet in order to use it later in the iteration
        DataSet<Tuple4<Integer, Point, Double, Double[]>>
                pointsFromLastIteration = loop.filter(new PointFilter()).project(1, 2, 3, 4);

        // Separate centroids in a DataSet in order to use it later in the iteration
        DataSet<Centroid>
                centroidsFromLastIteration = loop.filter(new CentroidFilter()).map(new ExtractCentroids());

        // Separate COI in a DataSet in order to use it later in the iteration
        DataSet<COI>
                coiFromLastIteration = loop.filter(new COIFilter()).map(new ExtractCOI());

        // Asssigning each point to the nearest centroid
        DataSet<Tuple4<Integer, Point, Double, Double[]>> partialClusteredPoints = pointsFromLastIteration
                // Compute closest centroid for each point
                .map(new SelectNearestCenter())
                .withBroadcastSet(centroidsFromLastIteration, "centroids")
                .withBroadcastSet(coiFromLastIteration, "coi");

        // Producing new centroids based on the clustered points
        DataSet<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> centroidsToNextIteration = partialClusteredPoints
                // Count and sum point coordinates for each centroid
                .map(new CountAppender())
                .groupBy(0).reduce(new CentroidAccumulator())
                // Compute new centroids from point counts and coordinate sums
                .map(new CentroidAverager());

        // Expand the tuples with points from a Tuple4 to a Tuple7 in order to union it with other datasets
        DataSet<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>>
                pointsToNextIteration = partialClusteredPoints.map(new ExpandPointsTuple());

        // Separate out centroids in order to be used in calculation for COI
        DataSet<Centroid> singleNewCentroids = centroidsToNextIteration.filter(new CentroidFilter())
                .map(new ExtractCentroids());

        // Computing the new COI information
        DataSet<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>>
                coiToNextIteration = singleNewCentroids.reduceGroup(new computeCOIInformation())
                // Broadcast data that is needed in the initial clustering to each node
                .withBroadcastSet(centroidsFromLastIteration, "oldCentroids");

        // Check if the algorithm has converged. If no centroids has moved more than 0.001, an empty DataSet will be
        // returned and that will halt the iteration.
        DataSet<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> converged = coiToNextIteration.filter(new checkConvergenceFilter());

        // Combine points, centroids and coi DataSets to one DataSet
        DataSet<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>>
                toNextIteration = pointsToNextIteration.union(centroidsToNextIteration.union(coiToNextIteration));

        // Ending the loop and feeding back DataSet
        DataSet<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> finalOutput = loop.closeWith(toNextIteration, converged);

        // ************************************************************************************************************
        //  Loop ends here. Feed new centroids back into next iteration
        // ************************************************************************************************************


        // Only preserve the information (ID of cluster point is assigned to and the point itself) that will be
        // printed to file or the console
        DataSet<Tuple2<Integer, Point>> clusteredPoints = finalOutput
                .filter(new PointFilter())
                .project(1, 2);

        // Print the results, either to a file of the console
        if (params.has("output")) {
            clusteredPoints.writeAsCsv(params.get("output"), "\n", " ").setParallelism(1);

            // Calling execute will trigger the execution of the file sink (file sinks are lazy)
            JobExecutionResult executionResult = env.execute("KMeansTI");
            int c = executionResult.getAccumulatorResult("num-distance-calculations");
            System.out.println("Number of distance calculations: " + c);
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");

            clusteredPoints.print();
        }
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /**
     * This class implements the FilterFunction in order to filter out Tuple7's with Centroid
     */
    public static class CentroidFilter implements FilterFunction<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> {

        /**
         * Filter out Tuple7 that does not have the key 0, whcih means that the Tuple does not contain a
         * Centroid object.
         *
         * @param unionData Tuple7 with all unionData
         * @return boolean True if f0-field (key) is equal to 0, else return False
         */
        @Override
        public boolean filter(Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI> unionData) {
            return (unionData.f0 == 0);
        }
    }

    /**
     * This class implements the FilterFunction in order to filter out Tuple7's with Point
     */
    public static class PointFilter implements FilterFunction<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> {

        /**
         * Filter out Tuple7 that does not have the key 0, whcih means that the Tuple does not contain a
         * Point object.
         *
         * @param unionData Tuple7 with all unionData
         * @return boolean True if f0-field (key) is equal to 1, else return False
         */
        @Override
        public boolean filter(Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI> unionData) {
            return (unionData.f0 == 1);
        }
    }

    /**
     * This class implements the FilterFunction in order to filter out Tuple7's with COI
     */
    public static class COIFilter implements FilterFunction<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> {

        /**
         * Filter out Tuple7 that does not have the key 2, which means the Tuple does not contain a COI object.
         *
         * @param unionData Tuple7 with all unionData.
         * @return boolean True if f0-field (key) is equal to 2, else return False.
         */
        @Override
        public boolean filter(Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI> unionData) {
            return (unionData.f0 == 2);
        }
    }

    /**
     * This class implements the FilterFunction in order to filter out a COI object based on convergence. A returned
     * COI object means that the algorithm has not converged.
     */
    public static class checkConvergenceFilter implements FilterFunction<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> {

        /**
         * Filter out and return the COI object if not all centroids has converged (meaning that they have moved more
         * than 0.01). A returned object will allows the iteration to continue. If no object is returned and the
         * DataSet is empty, the iteration will stop.
         *
         * @param COITuple Tuple7 with one COI object.
         * @return boolean True if not converged, False if it has converged
         */
        @Override
        public boolean filter(Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI> COITuple) {

            double[] oldNewCentroidDistances = COITuple.f6.distMap;
            boolean hasConverged = false;

            // Loop trough all oldNewCentroidDistances to check for convergence
            for (double distance : oldNewCentroidDistances) {

                // Checking if one of the centroids has moved more than 0.01.
                // If one has, the algorithm has not converged
                if (distance > 0.01) {
                    hasConverged = true;

                    // Jump out of the loop and return result
                    break;
                }
            }

            return hasConverged;
        }
    }

    /**
     * This class implements the MapFunction to extract the COI object from a tuple
     */
    public static class ExtractCOI implements MapFunction<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>, COI> {

        /**
         * Takes a Tuple7 that includes the COI object and return only this object. In this implementation no more
         * than one COI object should exist at any time.
         *
         * @param COITuple Tuple7 with a COI object
         * @return COI, should only be one object that is returned.
         */
        @Override
        public COI map(Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI> COITuple) {
            return COITuple.f6;
        }
    }

    /**
     * This class implements the MapFunction to extract all Centroids objects from the tuples
     */
    public static class ExtractCentroids implements MapFunction<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>, Centroid> {

        /**
         * Takes a Tuple7 with all information and extracts only the Centroid object. Multiple centroids exists,
         * however the number of centroids is usually relatively low.
         *
         * @param unionData Tuple 7 with all information
         * @return Centroid
         */
        @Override
        public Centroid map(Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI> unionData) {
            return unionData.f5;
        }
    }

    /**
     * This class implements the MapFunction to expand the points tuple
     */
    public static class ExpandPointsTuple implements MapFunction<Tuple4<Integer, Point, Double, Double[]>, Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> {

        /**
         * Takes a Tuple4 and return a Tuple7 where the fields that are not used by points is empty. This Tuple7
         * is used to union all data at the end of the iteration.
         *
         * @param point A Tuple4 with the ID the point is assigned to, the point itself, the upper bound and the lower
         *              bounds
         * @return Tuple7 with correct key identifier (1) and the rest of the information.
         */
        @Override
        public Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>
            map(Tuple4<Integer, Point, Double, Double[]> point) {

            return new Tuple7<>(1, point.f0, point.f1, point.f2, point.f3, null, null);
        }
    }

    /**
     * This class implements the RichMapFunction to initially select the nearest centroid to a point. This class
     * function is utilized once before the loop begins. Field f1 is forwarded to improve efficiency, as this field
     * is not changed in the function.
     */
    @ForwardedFields("f1")
    public static final class SelectInitialNearestCenter extends RichMapFunction<Tuple4<Integer, Point, Double, Double[]>, Tuple4<Integer, Point, Double, Double[]>> {
        private Collection<Centroid> centroids;
        private Collection<COI> coiCollection;

        // Accumulator used to track the total number of distance calculations performed
        private IntCounter numDistanceCalculations = new IntCounter();

        /**
         * Reads the centroid values from a broadcast DataSet and reads the COI value from a broadcast DataSet
         *
         * @param parameters The runtime parameters
         */
        @Override
        public void open(Configuration parameters) {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
            this.coiCollection = getRuntimeContext().getBroadcastVariable("coi");

            // Registering the accumulator object and defining the name of the accumulator
            getRuntimeContext().addAccumulator("num-distance-calculations", this.numDistanceCalculations);
        }

        /**
         * This function select the initial centroids each point is assigned to. It also calculates the lower bounds
         * and the upper bound.
         *
         * @param tuple A Tuple4 with 0 as the initial centroid ID the point is assigned to. This 0 will be overwritten
         * @return A Tuple4 with nearest centroid ID
         */
        @Override
        public Tuple4<Integer, Point, Double, Double[]> map(Tuple4<Integer, Point, Double, Double[]> tuple) {

            // The COI object is extracted to a single object
            COI coi = new ArrayList<>(coiCollection).get(0);

            Point point = tuple.f1;
            Double[] lb = tuple.f3;
            Centroid c = centroids.iterator().next();

            // Calculating the distance between the first centroid in the collection and this point
            double minDistance = point.euclideanDistance(c);

            // Increasing the accumulator for number of distance calculations
            this.numDistanceCalculations.add(1);

            double dist;
            int closestCentroidId = c.id;

            lb[closestCentroidId-1] = minDistance;

            // Loop trough all cluster centers
            for (Centroid centroid : centroids) {

                if(0.5 * coi.iCD[closestCentroidId-1][centroid.id-1] < minDistance) {

                    // Calculating the lower bound for this centroid and saving the distance between the centroid
                    // and this point
                    lb[centroid.id-1] = dist = point.euclideanDistance(centroid);

                    // Increasing the accumulator for number of distance calculations
                    this.numDistanceCalculations.add(1);

                    if(dist < minDistance) {
                        minDistance = dist;
                        closestCentroidId = centroid.id;
                    }
                }
            }

            Double ub = minDistance;

            // Emit a new record with the current closest center ID and the data point.
            return new Tuple4<>(closestCentroidId, point, ub, lb);
        }
    }

    /**
     * This class implements the RichMapFunction to select the nearest cluster center for a point. This class function
     * is utilized within the iteration. Field f1 is forwarded as this is not changed.
     */
    @ForwardedFields("f1")
    public static final class SelectNearestCenter extends RichMapFunction<Tuple4<Integer, Point, Double, Double[]>, Tuple4<Integer, Point, Double, Double[]>> {
        private Collection<Centroid> centroids;
        private Collection<COI> coiCollection;

        /**
         * Reads the centroid values from a broadcast DataSet and reads the COI value from a broadcast DataSet
         * @param parameters The runtime parameters
         */
        @Override
        public void open(Configuration parameters) {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
            this.coiCollection = getRuntimeContext().getBroadcastVariable("coi");
        }

        /**
         * This function takes a clustered point and assigns it to a new centroid if a centroid is deemed to be
         * closer than the already assigned centroid. The lower bounds and the upper bound is updated as well.
         *
         * @param tuple A Tuple4 with an ID for assigned centroid, the point itself, upper bound and the lower bounds
         * @return A Tuple4 with possibly new ID for assigned centroid, the point itself, upper bound and the
         *          lower bounds
         */
        @Override
        public Tuple4<Integer, Point, Double, Double[]> map(Tuple4<Integer, Point, Double, Double[]> tuple) {

            // Unpacking the COI object
            COI coi = new ArrayList<>(coiCollection).get(0);

            // Unpacking the centroids
            Centroid[] centroidArray =  centroids.toArray(new Centroid[0]);
            Point point = tuple.f1;

            Integer closestCentroidId = tuple.f0;
            boolean upperBoundUpdated = false;

            Double[] currentLb = tuple.f3;
            Double currentUb = tuple.f2;

            Double[] newLb = currentLb;
            Double newUb = currentUb;

            // Calculating k new lower bounds
            for (int i = 0; i < coi.k - 1; i++) {
                newLb[i] = Math.max((currentLb[i] - coi.distMap[i]), 0.0);
            }

            // Checking if the upperBound need to get updated
            if (coi.distMap[closestCentroidId - 1] > 0.0) {

                // Updating the upperBound by adding the distance the currently assigned centroid has moved
                newUb = currentUb + coi.distMap[closestCentroidId - 1];
                upperBoundUpdated = true;
                System.out.println("Updating UB ");
            }

            double dist1;
            double dist2;

            if (newUb > coi.minCD[closestCentroidId - 1]) {

                // check all cluster centers
                for (Centroid centroid : centroids) {

                    // Check if this centroid ID is not current assigned centroid ID
                    // Check if upper bound is greater than this points lower bound for centroid
                    // Check if DISTANCE(THIS CENTROID AND P'S CURRENT ASSIGNED CENTROID)  >= 2 * MIN_DIST
                    if ((centroid.id != closestCentroidId) && (newUb > newLb[centroid.id-1]) && (newUb > coi.iCD[closestCentroidId-1][centroid.id-1])) {

                        // Do only this if upper bound is updated
                        if (upperBoundUpdated) {
                            dist1 = point.euclideanDistance(centroidArray[closestCentroidId-1]);
                            newUb = dist1;
                            newLb[closestCentroidId-1] = dist1;
                            upperBoundUpdated = false;
                        } else {
                            dist1 = newUb;
                        }

                        // TODO: More comments around her aswell
                        if (dist1 > newLb[centroid.id-1] || (dist1 > (0.5 * coi.iCD[closestCentroidId-1][centroid.id-1]))) {
                            dist2 = point.euclideanDistance(centroid);
                            newLb[centroid.id-1] = dist2;

                            if(dist2 < dist1) {
                                closestCentroidId = centroid.id;

                                newUb = dist2;
                                upperBoundUpdated = false;
                            }
                        }
                    }
                }
            }

            // emit a new record with the center id and the data point.
            return new Tuple4<>(closestCentroidId, tuple.f1, newUb, newLb);
        }
    }

    /**
     * This class implements the MapFunction to append an integer to each point, which is used to count total
     * occurrences of points assigned to one centroid. Field f0 (the ID of the cluster) and field f1 (the point)
     * is forwarded directly to the input.
     */
    @ForwardedFields("f0;f1")
    public static final class CountAppender implements MapFunction<Tuple4<Integer, Point, Double, Double[]>, Tuple3<Integer, Point, Long>> {

        /**
         * Takes in a Tuple4 with all point information and return a Tuple3 with the ID of the cluster the point is
         * assigned to, the point itself and a counter variable. The upper and lower bounds are removed since they are
         * not needed in the calculation of the new centroids (it would complicate the use of the reduce
         * function later if they were preserved also)
         *
         * @param pointData A Tuple4 with all point information
         * @return A Tuple3 with only necessary data and a counter
         */
        @Override
        public Tuple3<Integer, Point, Long> map(Tuple4<Integer, Point, Double, Double[]> pointData) {
            return new Tuple3<>(pointData.f0, pointData.f1, 1L);
        }
    }

    /**
     * This class implements the ReduceFunction in order to take in two points and reduce them to one, by adding the
     * points and the counters. Field 0 is forwarded as this does not change.
     */
    @ForwardedFields("0")
    public static final class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Point, Long>> {

        /**
         * Takes two points and add them together, in addition to adding the counters together. When all points are
         * accumulated the new centroid can be calculated in a later function using output from this function.
         *
         * @param point1 First point to be reduced to one
         * @param point2 Second point to be reduced to
         * @return A single Tuple3 is returned with the total values from the two points.
         */
        @Override
        public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> point1, Tuple3<Integer, Point, Long> point2) {
            return new Tuple3<>(point1.f0, point1.f1.add(point2.f1), point1.f2 + point2.f2);
        }
    }

    /**
     * This class implements the MapFunction in order to compute a new centroid based on a fully accumulated point
     */
    public static final class CentroidAverager implements MapFunction<Tuple3<Integer, Point, Long>, Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> {

        /**
         * Takes in a fully accumulated point and return the centroid which is the average of all current points
         * assigned to the centroid.
         *
         * @param value The accumulated point
         * @return Tuple7 with all centroid field filled and all other fields contain dummy data. The key for the tuple
         *          is also set to 0, indicating that it holds a centroid.
         */
        @Override
        public Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI> map(Tuple3<Integer, Point, Long> value) {
            Centroid centroid = new Centroid(value.f0, value.f1.div(value.f2));

            // Initilazing empty values to put in the tuple
            Double emptyDouble = 0.0;
            Double[] emptyDoubleArray = {0.0};

            return new Tuple7<>(0, 0, null, emptyDouble, emptyDoubleArray, centroid, null);
        }
    }

    /**
     * This class implements the RichGroupReduceFunction in order to compute the COI (Carry-Over-Information) object.
     * The COI object contains information about the inter-centroid distances (distances between each centroid),
     * how much each centroid has moved between last and current iteration, the minimum distance between each centroid,
     * and what K is.
     */
    public static class computeCOIInformation extends RichGroupReduceFunction<Centroid, Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> {
        private Collection<Centroid> centroidCollection;

        /**
         * Reads the centroid values from a broadcast DataSet into a collection.
         *
         * @param parameters The runtime parameters
         */
        @Override
        public void open(Configuration parameters) {
            this.centroidCollection = getRuntimeContext().getBroadcastVariable("oldCentroids");
        }

        /**
         * This function use the centroid from the last iteration and the centroids from this iteration in order to
         * produce the COI (Carry-Over-Information) object. The old centroids are passed to this function via a
         * broadcast variable, while the new centroids are passed directly to the function trough its input (since
         * it is a GroupReduceFunction that reduce the input from a whole group).
         *
         * @param iterable This is the centroids from the current iteration, all sent to the group reduce function
         * @param collector Tuple7 where the COI object is stored, the correct key is set and returned
         */
        @Override
        public void reduce(Iterable<Centroid> iterable, Collector<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> collector) {
            
            // Instantiate list to store current / new centroids
            List<Centroid> newCentroids = new ArrayList<>();

            // Convert Collection centroidCollection to ArrayList
            List<Centroid> oldCentroids = new ArrayList<>(centroidCollection);

            // Convert Iterable iterable to ArrayList by adding to existing list instantiated above
            for(Centroid c : iterable) {
                newCentroids.add(c);
            }

            // Store the size of the List, which is used in allocation of array below
            int dims = newCentroids.size();

            // Ensure that the centroids are sorted in ascending order on their ID in order to be able to use
            // them in the for-loop below.
            Collections.sort(newCentroids);
            Collections.sort(oldCentroids);

            // Allocate the multidimensional array
            double[][] matrix = new double[dims][dims];
            double[] minCD = new double[dims];
            double[] distMap = new double[dims];

            // Computes the distances between every centroid and place them in List l
            for(int i = 0; i < newCentroids.size(); i++) {
                double minVal = Double.MAX_VALUE;

                // This represents the outer centroid
                Centroid ci = newCentroids.get(i);

                for(int j = 0; j < newCentroids.size(); j++) {

                    // Check that k != k'
                    if (i != j) {

                        // This represents the inner centroid
                        Centroid cj = newCentroids.get(j);

                        // Calculate the distance between the two centroids
                        double dist = ci.euclideanDistance(cj);

                        // Update the matrix with the distance
                        matrix[i][j] = dist;

                        // Look for the smallest value and update if smaller
                        if (dist < minVal) {
                            minVal = dist;
                        }

                    } else {
                        // If k == k', distance is automatically 0
                        matrix[i][j] = 0;
                    }
                }

                // Update minCD with 0.5 of minVal
                minCD[i] = (minVal / 2);
            }

            // Produce the distMap
            for (int i = 0; i < dims; i++) {
                distMap[i] = newCentroids.get(i).euclideanDistance(oldCentroids.get(i));
            }

            // Make the new COI object
            COI coi = new COI(matrix, minCD, distMap);

            // Initilazing empty values to put in the tuple
            Double emptyDouble = 0.0;
            Double[] emptyDoubleArray = {0.0};

            Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI> coiTuple = new Tuple7<>(2, 0, null, emptyDouble, emptyDoubleArray,  null, coi);

            // Add it to the collector which will return it
            collector.collect(coiTuple);
        }
    }
}
