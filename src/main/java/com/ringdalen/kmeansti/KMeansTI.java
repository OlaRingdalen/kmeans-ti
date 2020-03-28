package com.ringdalen.kmeansti;

import com.ringdalen.kmeansti.util.DataTypes.Point;
import com.ringdalen.kmeansti.util.DataTypes.Centroid;
import com.ringdalen.kmeansti.util.DataTypes.COI;

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

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;


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

    private static final Logger LOG = LoggerFactory.getLogger(KMeansTI.class);

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up execution environment
        //ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // Read both points and centroids form files
        DataSet<Tuple4<Integer, Point, Double, Double[]>> points = getPointDataSet(params, env);
        DataSet<Centroid> centroids = getCentroidDataSet(params, env);

        // Fetching max number of iterations loop is executed with
        int iterations = params.getInt("iterations", 10);

        // Computing the COI information
        DataSet<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> coiTuple = centroids.reduceGroup(new computeCOIInformation())
                .withBroadcastSet(centroids, "oldCentroids");

        DataSet<COI> coi = coiTuple.map(new ExtractCOI());

        //////////////////////////////////////// Initial mapping of the points ////////////////////////////////////////
                DataSet<Tuple4<Integer, Point, Double, Double[]>> initialClusteredPoints = points
                .map(new SelectInitialNearestCenter())
                .withBroadcastSet(centroids, "centroids")
                .withBroadcastSet(coi, "coi");
        //////////////////////////////////////// Initial mapping of the points ////////////////////////////////////////

        // Producing new centroids based on the initial clustered points
        DataSet<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> initialCentroidsTuple = initialClusteredPoints
                // Count and sum point coordinates for each centroid
                .map(new CountAppender())
                .groupBy(0).reduce(new CentroidAccumulator())
                // Compute new centroids from point counts and coordinate sums
                .map(new CentroidAverager());



        DataSet<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> initialPointsTuple = initialClusteredPoints.map(new ExpandPointsTuple());
        //DataSet<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> initialCentroidsTuple = centroids.map(new ExpandCentroidsTuple());

        // Combine the points and centroids DataSets to one DataSet
        DataSet<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> unionData = initialPointsTuple.union(initialCentroidsTuple.union(coiTuple));

        // ************************************************************************************************************
        //  Loop begins here
        // ************************************************************************************************************

        // Use unionData to iterate on for n iterations, as specified in the input arguments
        IterativeDataSet<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> loop = unionData.iterate(iterations);

        // Separate out points, centroids and COI information to separate DataSets in order to perform computations on them
        DataSet<Tuple4<Integer, Point, Double, Double[]>> pointsFromLastIteration = loop.filter(new PointFilter()).project(1, 2, 3, 4);
        DataSet<Centroid> centroidsFromLastIteration = loop.filter(new CentroidFilter()).map(new ExtractCentroids());
        DataSet<COI> coiFromLastIteration = loop.filter(new COIFilter()).map(new ExtractCOI());

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

        DataSet<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> pointsToNextIteration = partialClusteredPoints.map(new ExpandPointsTuple());

        // Separate out centroids in order to be used in calculation for COI
        DataSet<Centroid> singleNewCentroids = centroidsToNextIteration.filter(new CentroidFilter()).map(new ExtractCentroids());

        // Computing the new COI information
        DataSet<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>>
                coiToNextIteration = singleNewCentroids.reduceGroup(new computeCOIInformation())
                .withBroadcastSet(centroidsFromLastIteration, "oldCentroids");

        DataSet<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> converged = coiToNextIteration.filter(new checkConvergenceFilter());

        // Combine points, centroids and coi DataSets to one DataSet
        DataSet<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>>
                toNextIteration = pointsToNextIteration.union(centroidsToNextIteration.union(coiToNextIteration));

        // Ending the loop and feeding back DataSet
        DataSet<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> finalOutput = loop.closeWith(toNextIteration, converged);

        // ************************************************************************************************************
        //  Loop ends here. Feed new centroids back into next iteration
        // ************************************************************************************************************

        DataSet<Tuple2<Integer, Point>> clusteredPoints = finalOutput.filter(new PointFilter()).project(1, 2);


        // emit result
        if (params.has("output")) {
            clusteredPoints.writeAsCsv(params.get("output"), "\n", " ");

            // since file sinks are lazy, we trigger the execution explicitly
            env.execute("KMeans Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            clusteredPoints.print();
        }
    }

    // *************************************************************************
    //     DATA SOURCE READING (POINTS AND CENTROIDS)
    // *************************************************************************

    /**
     * Function to map data from a file to Centroid objects
     */
    private static DataSet<Centroid> getCentroidDataSet(ParameterTool params, ExecutionEnvironment env) {

        DataSet<Centroid> centroids;

        // Parsing d features, plus the ID (thats why the +1 is included) from file to Centroid objects
        centroids = env.readTextFile(params.get("centroids"))
                .map(new ReadCentroidData(params.getInt("d")));

        return centroids;
    }

    /**
     * Function to map data from a file to Point objects
     */
    private static DataSet<Tuple4<Integer, Point, Double, Double[]>> getPointDataSet(ParameterTool params, ExecutionEnvironment env) {

        DataSet<Tuple4<Integer, Point, Double, Double[]>> points;

        // Parsing d features from file to Point objects
        points = env.readTextFile(params.get("points"))
                .map(new ReadPointData(params.getInt("d"), params.getInt("k")));

        return points;
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
        public boolean filter(Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI> unionData) throws Exception {
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
        public boolean filter(Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI> unionData) throws Exception {
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
         * @param unionData Tuple7 with all unionData
         * @return boolean True if f0-field (key) is equal to 2, else return False
         */
        @Override
        public boolean filter(Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI> unionData) throws Exception {
            return (unionData.f0 == 2);
        }
    }

    public static class checkConvergenceFilter implements FilterFunction<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> {
        @Override
        public boolean filter(Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI> tuple) throws Exception {

            double[] oldNewCentroidDistance = tuple.f6.distMap;

            boolean converged = true;

            for(int i = 0; i < oldNewCentroidDistance.length; i++) {
                if(oldNewCentroidDistance[i] > 0.01) {
                    converged = false;
                }
            }

            return !converged;
        }
    }

    public static class ExtractCOI implements MapFunction<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>, COI> {

        @Override
        public COI map(Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI> COIData) throws Exception {
            return COIData.f6;
        }
    }

    public static class ExtractCentroids implements MapFunction<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>, Centroid> {

        @Override
        public Centroid map(Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI> unionData) throws Exception {
            return unionData.f5;
        }
    }

    public static class ExpandPointsTuple implements MapFunction<Tuple4<Integer, Point, Double, Double[]>, Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> {

        @Override
        public Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>
            map(Tuple4<Integer, Point, Double, Double[]> point) throws Exception {

            return new Tuple7<>(1, point.f0, point.f1, point.f2, point.f3, null, null);
        }
    }

    /**
     * Tuple fields cannot be 0 here, as this will throw an error when the tuples cannot be serialized
     */
    public static class ExpandCentroidsTuple implements MapFunction<Centroid, Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> {

        @Override
        public Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI> map(Centroid centroid) throws Exception {

            // Initilazing empty values to put in the tuple
            Double emptyDouble = 0.0;
            Double[] emptyDoubleArray = {0.0};

            return new Tuple7<>(0, 0, null, emptyDouble, emptyDoubleArray, centroid, null);
        }
    }

    /** Reads the input data and generate points */
    public static class ReadPointData implements MapFunction<String, Tuple4<Integer, Point, Double, Double[]>> {
        double[] row;
        int k;

        public ReadPointData(int d, int k){
            this.row = new double[d];
            this.k = k;
        }

        @Override
        public Tuple4<Integer, Point, Double, Double[]> map(String s) throws Exception {
            String[] buffer = s.split(" ");

            // Extracting values from the input string
            for(int i = 0; i < row.length; i++) {
                row[i] = Double.parseDouble(buffer[i]);
            }

            // Declaring the initial upper bound to -1
            Double ub = -1.0;

            // Declaring the initial lower bounds to -1
            Double[] lb = new Double[k];
            Arrays.fill(lb, 0.0);

            return new Tuple4<>(-1, new Point(row), ub, lb);
        }
    }

    /** Reads the input data and generate centroids */
    public static class ReadCentroidData implements MapFunction<String, Centroid> {
        double[] row;

        public ReadCentroidData(int d){
            //System.out.println("D is of length: " + d);
            row = new double[d];
        }

        @Override
        public Centroid map(String s) throws Exception {
            String[] buffer = s.split(" ");
            int id = Integer.parseInt(buffer[0]);

            // buffer is +1 since this array is one longer
            for(int i = 0; i < row.length; i++) {
                row[i] = Double.parseDouble(buffer[i+1]);
            }

            return new Centroid(id, row);
        }
    }

    /** Determines the initial closest cluster to data point. */
    @ForwardedFields("f1")
    public static final class SelectInitialNearestCenter2 extends RichMapFunction<Tuple4<Integer, Point, Double, Double[]>, Tuple4<Integer, Point, Double, Double[]>> {
        private Collection<Centroid> centroids;
        private Collection<COI> coiCollection;

        /** Reads the centroid values from a broadcast variable into a collection. */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
            this.coiCollection = getRuntimeContext().getBroadcastVariable("coi");
        }

        @Override
        public Tuple4<Integer, Point, Double, Double[]> map(Tuple4<Integer, Point, Double, Double[]> tuple) throws Exception {

            // UNPACK COI HERE
            COI coi = new ArrayList<>(coiCollection).get(0);
            Point point = tuple.f1;

            double minDistance = Double.MAX_VALUE;
            int closestCentroidId = -1;
            int k = centroids.size();

            Double ub = tuple.f2;
            Double[] lb = tuple.f3;

            // All values defaults to false when initialized
            boolean[] skip = new boolean[k];

            // Loop trough all cluster centers
            for (Centroid centroid : centroids) {

                // This is the ID of the centroid, minus one. Used to reference position in arrays
                int centroidRef = centroid.id - 1;

                // Check if this distance calculation can be avoided based on calculation in previous iteration
                //System.out.println("Skip is: " + skip[0]);
                if (!skip[centroidRef]) {

                    // Compute distance
                    double distance = point.euclideanDistance(centroid);

                    if(distance < 0.0) {
                        System.out.println("Found minus distance");
                    }

                    // Setting the lower bound (LB)
                    lb[centroidRef] = distance;

                    // Update nearest cluster if necessary
                    if (distance < minDistance) {
                        minDistance = distance;

                        // Setting the upper bound (UB)
                        ub = minDistance;

                        closestCentroidId = centroid.id;

                        // Only loops trough the upper diagonal of the iCD multidimensional array, within this
                        // outer loop.
                        for (int i = centroidRef; i < k-1; i++) {

                            // If d(centroid, centroid') >= 2d(point, centroid)
                            // Then d(point, centroid') >= d(point, centroid)
                            if (coi.iCD[centroidRef][i] >= (2 * distance)) {
                                skip[i] = true;
                            }
                        }
                    }
                }
            }

            //System.out.println("Point: " + point + "\t(UB: " + ub + ")" + "(LB: " + lb[0] + ", " + lb[1] + ", " + lb[2] + ")");

            // Emit a new record with the current closest center ID and the data point.
            return new Tuple4<>(closestCentroidId, point, ub, lb);
        }
    }

    /** EXPERIMENTAL VERSION: Determines the initial closest cluster to data point. */
    @ForwardedFields("f1")
    public static final class SelectInitialNearestCenter extends RichMapFunction<Tuple4<Integer, Point, Double, Double[]>, Tuple4<Integer, Point, Double, Double[]>> {
        private Collection<Centroid> centroids;
        private Collection<COI> coiCollection;

        /** Reads the centroid values from a broadcast variable into a collection. */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
            this.coiCollection = getRuntimeContext().getBroadcastVariable("coi");
        }

        @Override
        public Tuple4<Integer, Point, Double, Double[]> map(Tuple4<Integer, Point, Double, Double[]> tuple) throws Exception {

            // UNPACK COI HERE
            COI coi = new ArrayList<>(coiCollection).get(0);
            Point point = tuple.f1;

            double minDistance = Double.MAX_VALUE;
            int closestCentroidId = -1;
            int k = centroids.size();

            Double ub = tuple.f2;
            Double[] lb = tuple.f3;

            Centroid c = centroids.iterator().next();

            minDistance = point.euclideanDistance(c);
            double dist = minDistance;
            closestCentroidId = c.id;

            lb[closestCentroidId-1] = minDistance;

            // Loop trough all cluster centers
            for (Centroid centroid : centroids) {

                if(0.5 * coi.iCD[closestCentroidId-1][centroid.id-1] < minDistance) {
                    lb[centroid.id-1] = dist = point.euclideanDistance(centroid);

                    if(dist < minDistance) {
                        minDistance = dist;
                        closestCentroidId = centroid.id;
                    }
                }

            }

            ub = minDistance;

            //System.out.println("Point: " + point + "\t(UB: " + ub + ")" + "(LB: " + lb[0] + ", " + lb[1] + ", " + lb[2] + ")");

            //System.out.println("Initially assigning " + point + " to centroid " + closestCentroidId);

            // Emit a new record with the current closest center ID and the data point.
            return new Tuple4<>(closestCentroidId, point, ub, lb);
        }
    }

    /** Determines the closest cluster center for a data point. */
    //@ForwardedFields("f1")
    public static final class SelectNearestCenter extends RichMapFunction<Tuple4<Integer, Point, Double, Double[]>, Tuple4<Integer, Point, Double, Double[]>> {
        private Collection<Centroid> centroids;
        private Collection<COI> coiCollection;

        /** Reads the centroid values from a broadcast variable into a collection. */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
            this.coiCollection = getRuntimeContext().getBroadcastVariable("coi");
        }

        @Override
        public Tuple4<Integer, Point, Double, Double[]> map(Tuple4<Integer, Point, Double, Double[]> tuple) throws Exception {

            // Unpacking the COI object
            COI coi = new ArrayList<>(coiCollection).get(0);

            // Unpacking the centroids
            Centroid[] centroidArray =  centroids.toArray(new Centroid[0]);
            Point point = tuple.f1;

            double minDistance = Double.MAX_VALUE;

            Integer closestCentroidId = tuple.f0;
            boolean upperBoundUpdated = false;

            Double[] currentLb = tuple.f3;
            Double currentUb = tuple.f2;

            Integer centroidID = tuple.f0;

            Double[] newLb = currentLb;
            Double newUb = currentUb;

            // Calculating k new lower bounds
            for (int i = 0; i < coi.k - 1; i++) {
                newLb[i] = Math.max((currentLb[i] - coi.distMap[i]), 0.0);
            }

            //System.out.println("Length of coi.distMap is " + coi.distMap.length);
            //System.out.println("The ID that is used is " + (closestCentroidId-1));

            //System.out.println("Distance between old and new centroid is " + coi.distMap[closestCentroidId - 1] );

            // Checking if the upperBound need to get updated
            if (coi.distMap[closestCentroidId - 1] > 0.0) {

                // Updating the upperBound by adding the distance the currently assigned centroid has moved
                newUb = currentUb + coi.distMap[closestCentroidId - 1];
                upperBoundUpdated = true;
                System.out.println("Updating UB ");
            }

            double dist1 = 0.0;
            double dist2 = 0.0;

            if (newUb > coi.minCD[closestCentroidId - 1]) {

                // check all cluster centers
                for (Centroid centroid : centroids) {

                    // Check if this centroid ID is not current assigned centroid ID
                    // Check if upper bound is greater than this points lower bound for centroid
                    // Check if DISTANCE(THIS CENTROID AND P'S CURRENT ASSIGNED CENTROID)  >= 2 * MIN_DIST
                    if ((centroid.id != closestCentroidId) && (newUb > newLb[centroid.id-1]) && (newUb > coi.iCD[closestCentroidId-1][centroid.id-1])) {

                        // Do only this is upper bound is updated
                        if (upperBoundUpdated) {
                            dist1 = point.euclideanDistance(centroidArray[closestCentroidId-1]);
                            newUb = dist1;
                            newLb[closestCentroidId-1] = dist1;
                            upperBoundUpdated = false;
                        } else {
                            dist1 = newUb;
                        }

                        if (dist1 > newLb[centroid.id-1] || (dist1 > (0.5 * coi.iCD[closestCentroidId-1][centroid.id-1]))) {
                            dist2 = point.euclideanDistance(centroid);
                            newLb[centroid.id-1] = dist2;

                            if(dist2 < dist1) {
                                closestCentroidId = centroid.id;

                                //System.out.println("Assigning " + point + " to centroid " + closestCentroidId + ". Because " + dist2 + " is less than " + dist1 + "");
                                //System.out.println("Assigning " + point + " to centroid " + closestCentroidId + ". Because " + dist2 + " is less than " + dist1 + "");

                                newUb = dist2;
                                upperBoundUpdated = false;
                            }
                        }

                    } /*else {
                        System.out.println("This can be skipped");
                    }*/

                    // compute distance
                    //double distance = tuple.f1.euclideanDistance(centroid);

                    //System.out.println("AHEEM! Calculated distance between " + p.toString() + " and " + centroid.toString() + " is " + distance);

                    // update nearest cluster if necessary
                    //if (distance < minDistance) {
                     //   minDistance = distance;
                      //  closestCentroidId = centroid.id;
                    //}
                }
            }

            //System.out.println("Assigning " + point + " to centroid " + closestCentroidId);

            // emit a new record with the center id and the data point.
            return new Tuple4<>(closestCentroidId, tuple.f1, newUb, newLb);
        }
    }

    /**
     * Removed the upper and lower bounds since they are not needed in the calculation of the new centroids
     * Having them would make it more difficult to use reduce functions in the later stages.
     * Also appends a count variable to the tuple.
     */
    @ForwardedFields("f0")
    public static final class CountAppender implements MapFunction<Tuple4<Integer, Point, Double, Double[]>, Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer, Point, Long> map(Tuple4<Integer, Point, Double, Double[]> t) {
            return new Tuple3<>(t.f0, t.f1, 1L);
        }
    }

    /** Sums and counts point coordinates. */
    @ForwardedFields("0")
    public static final class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
            return new Tuple3<>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
        }
    }

    /** Computes new centroid from coordinate sum and count of points. */
    public static final class CentroidAverager implements MapFunction<Tuple3<Integer, Point, Long>, Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> {

        @Override
        public Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI> map(Tuple3<Integer, Point, Long> value) {
            Centroid centroid = new Centroid(value.f0, value.f1.div(value.f2));

            //System.out.println("CentroidAverager(): Centroid with ID " + value.f0);

            // Initilazing empty values to put in the tuple
            Double emptyDouble = 0.0;
            Double[] emptyDoubleArray = {0.0};

            return new Tuple7<>(0, 0, null, emptyDouble, emptyDoubleArray, centroid, null);
        }
    }

    /** Takes all centroids and computes centroid inter-distances */
    public static class computeCOIInformation extends RichGroupReduceFunction<Centroid, Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> {
        private Collection<Centroid> centroidCollection;

        /** Reads the centroid values from a broadcast variable into a collection. */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroidCollection = getRuntimeContext().getBroadcastVariable("oldCentroids");
            System.out.println("computeCOIInformation(): There are now " + (centroidCollection.size()) + " centroids." );
        }

        @Override
        public void reduce(Iterable<Centroid> iterable, Collector<Tuple7<Integer, Integer, Point, Double, Double[], Centroid, COI>> collector) throws Exception {
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

            // Ensure that the centroids are sorted in ascending order on their ID
            Collections.sort(newCentroids);
            Collections.sort(oldCentroids);

            // Allocate the multidimensional array
            double[][] matrix = new double[dims][dims];
            double[] minCD = new double[dims];
            double[] distMap = new double[dims];

            // USED FOR TESTING ONLY
            /*System.out.println("====================================== Printing newCentroids in order ====================================");
            for (int i = 0; i < dims; i++) {
                System.out.println(newCentroids.get(i));
            }
            System.out.println("====================================== Printing newCentroids in order ==================================");


            System.out.println("=================================== Printing oldCentroids in order =====================================");
            for (int i = 0; i < dims; i++) {
                System.out.println(oldCentroids.get(i));
            }
            System.out.println("====================================== Printing oldCentroids in order =====================================");

            System.out.println("Representation of centroids in COI function.");
            for(int i = 0; i < newCentroids.size(); i++) {
                System.out.print(newCentroids.get(i) + " - ");
            }
            System.out.println("\n");*/

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
            /*System.out.println("Producing the distMap");
            for (int i = 0; i < dims; i++) {
                distMap[i] = newCentroids.get(i).euclideanDistance(oldCentroids.get(i));
                System.out.println("Distance between " + newCentroids.get(i) + " and " + oldCentroids.get(i) + " is " + distMap[i]);
            }*/

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
