package com.ringdalen.kmeansti;

import com.ringdalen.kmeansti.util.DataTypes.Point;
import com.ringdalen.kmeansti.util.DataTypes.Centroid;
import com.ringdalen.kmeansti.util.DataTypes.COI;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

import java.io.Serializable;
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

        // get input data:
        // read the points and centroids from the provided paths or fall back to default data
        DataSet<Tuple5<Integer, Integer, Centroid, Point, COI>> points = getPointDataSet(params, env);
        DataSet<Tuple5<Integer, Integer, Centroid, Point, COI>> centroids = getCentroidDataSet(params, env);

        // Fetching the number of iterations that the program is executed with
        int iterations = params.getInt("iterations", 10);
        int dimension = params.getInt("d");

        // Computing the iCD
        /*DataSet<double[][]> matrix = centroids.reduceGroup(new computeCentroidInterDistance());

        LOG.error("Main executed");

        double[][] icd = matrix.collect().get(0);
        System.out.println("iCD er: ");

        for (int i = 0; i < icd.length; i++) {
            for (int j = 0; j < icd.length; j++) {
                System.out.print(icd[i][j] + "\t");
            }

            System.out.println("");
        }*/

        /** f0: Key; 0 -> Centroid, 1 -> Point, 2 -> COI
         *  f1: ID of cluster Point is assigned to
         *  f2: Centroid or null
         *  f3: Point or null
         *  f4: COI or null
         * */

        DataSet<Tuple5<Integer, Integer, Centroid, Point, COI>> allData = points.union(centroids);

        /*DataSet<Tuple2<Integer, Point>> nullClusteredPoints = points
                .map(new assignPointToNullCluster());*/

        // Loop begins here
        IterativeDataSet<Tuple5<Integer, Integer, Centroid, Point, COI>> loop = allData.iterate(iterations);

        DataSet<Tuple2<Integer, Point>> pointsFromLastIteration = loop.filter(new PointFilter()).project(1, 3);
        DataSet<Centroid> centroidsFromLastIteration = loop.filter(new CentroidFilter()).map(new ExtractCentroids());

        // Asssigning each point to the nearest centroid
        DataSet<Tuple2<Integer, Point>> partialClusteredPoints = pointsFromLastIteration
                // Compute closest centroid for each point
                .map(new SelectNearestCenter())
                .withBroadcastSet(centroidsFromLastIteration, "centroids");

        // Producing new centroids based on the clustered points
        DataSet<Tuple5<Integer, Integer, Centroid, Point, COI>> newCentroids = partialClusteredPoints
                // Count and sum point coordinates for each centroid
                .map(new CountAppender())
                .groupBy(0).reduce(new CentroidAccumulator())
                // Compute new centroids from point counts and coordinate sums
                .map(new CentroidAverager());

        DataSet<Tuple5<Integer, Integer, Centroid, Point, COI>> pointsToNextIteration = partialClusteredPoints.map(new ExpandPointsTuple());
        //DataSet<Tuple5<Integer, Integer, Centroid, Point, COI>> centroidsToNextIteration = newCentroids.map(new ExpandCentroidsTuple());

        DataSet<Tuple5<Integer, Integer, Centroid, Point, COI>> toNextIteration = pointsToNextIteration.union(newCentroids);

        // Loop ends here. Feed new centroids back into next iteration
        DataSet<Tuple5<Integer, Integer, Centroid, Point, COI>> finalOutput = loop.closeWith(toNextIteration);

        /*DataSet<Tuple2<Integer, Point>> clusteredPoints = nullClusteredPoints
                // assign points to final clusters
                .map(new SelectNearestCenter())
                .withBroadcastSet(finalCentroids, "centroids");*/

        DataSet<Tuple2<Integer, Point>> clusteredPoints = finalOutput.filter(new PointFilter()).project(1, 3);


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
    private static DataSet<Tuple5<Integer, Integer, Centroid, Point, COI>>
    getCentroidDataSet(ParameterTool params, ExecutionEnvironment env) {

        DataSet<Tuple5<Integer, Integer, Centroid, Point, COI>> centroids;

        // Parsing d features, plus the ID (thats why the +1 is included) from file to Centroid objects
        centroids = env.readTextFile(params.get("centroids"))
                .map(new ReadCentroidData(params.getInt("d")));

        return centroids;
    }

    /**
     * Function to map data from a file to Point objects
     */
    private static DataSet<Tuple5<Integer, Integer, Centroid, Point, COI>>
    getPointDataSet(ParameterTool params, ExecutionEnvironment env) {

        DataSet<Tuple5<Integer, Integer, Centroid, Point, COI>> points;

        // Parsing d features from file to Point objects
        points = env.readTextFile(params.get("points"))
                .map(new ReadPointData(params.getInt("d")));

        return points;
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    public static class ExtractCentroids implements MapFunction<Tuple5<Integer, Integer, Centroid, Point, COI>, Centroid> {

        @Override
        public Centroid map(Tuple5<Integer, Integer, Centroid, Point, COI> allData) throws Exception {
            return allData.f2;
        }
    }

    public static class PointFilter implements FilterFunction<Tuple5<Integer, Integer, Centroid, Point, COI>> {

        @Override
        public boolean filter(Tuple5<Integer, Integer, Centroid, Point, COI> allData) throws Exception {
            // Only return true if the key is equal to one, which means this is a point
            return (allData.f0 == 1);
        }
    }

    public static class CentroidFilter implements FilterFunction<Tuple5<Integer, Integer, Centroid, Point, COI>> {

        @Override
        public boolean filter(Tuple5<Integer, Integer, Centroid, Point, COI> allData) throws Exception {
            // Only return true if the key is equal to zero, which means this is a centroid
            return (allData.f0 == 0);
        }
    }

    public static class ExpandPointsTuple implements MapFunction<Tuple2<Integer, Point>, Tuple5<Integer, Integer, Centroid, Point, COI>> {

        @Override
        public Tuple5<Integer, Integer, Centroid, Point, COI> map(Tuple2<Integer, Point> point) throws Exception {

            return new Tuple5<>(1, point.f0, null, point.f1, null);
        }
    }

    public static class ExpandCentroidsTuple implements MapFunction<Centroid, Tuple5<Integer, Integer, Centroid, Point, COI>> {

        @Override
        public Tuple5<Integer, Integer, Centroid, Point, COI> map(Centroid centroid) throws Exception {

            return new Tuple5<>(1, 0, centroid, null, null);
        }
    }

    /** Reads the input data and generate points */
    public static class ReadPointData implements MapFunction<String, Tuple5<Integer, Integer, Centroid, Point, COI>> {
        double[] row;

        public ReadPointData(int d){
            row = new double[d];
        }

        @Override
        public Tuple5<Integer, Integer, Centroid, Point, COI> map(String s) throws Exception {
            String[] buffer = s.split(" ");

            for(int i = 0; i < row.length; i++) {
                row[i] = Double.parseDouble(buffer[i]);
            }

            //return new Point(row);
            return new Tuple5<>(1, 0, null, new Point(row), null);
        }
    }

    /** Reads the input data and generate centroids */
    public static class ReadCentroidData implements MapFunction<String, Tuple5<Integer, Integer, Centroid, Point, COI>> {
        double[] row;

        public ReadCentroidData(int d){
            //System.out.println("D is of length: " + d);
            row = new double[d];
        }

        @Override
        public Tuple5<Integer, Integer, Centroid, Point, COI> map(String s) throws Exception {
            String[] buffer = s.split(" ");
            int id = Integer.parseInt(buffer[0]);

            // buffer is +1 since this array is one longer
            for(int i = 0; i < row.length; i++) {
                row[i] = Double.parseDouble(buffer[i+1]);
            }

            //return new Centroid(id, row);
            return new Tuple5<>(0, 0, new Centroid(id, row), null, null);
        }
    }

    /** Determines the closest cluster center for a data point. */
    @ForwardedFields("f1")
    public static final class SelectNearestCenter extends RichMapFunction<Tuple2<Integer, Point>, Tuple2<Integer, Point>> {
        private Collection<Centroid> centroids;

        /** Reads the centroid values from a broadcast variable into a collection. */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public Tuple2<Integer, Point> map(Tuple2<Integer, Point> p) throws Exception {

            double minDistance = Double.MAX_VALUE;
            int closestCentroidId = -1;

            // check all cluster centers
            for (Centroid centroid : centroids) {
                // compute distance
                double distance = p.f1.euclideanDistance(centroid);

                //System.out.println("AHEEM! Calculated distance between " + p.toString() + " and " + centroid.toString() + " is " + distance);

                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = centroid.id;
                }
            }

            // emit a new record with the center id and the data point.
            return new Tuple2<>(closestCentroidId, p.f1);
        }
    }

    /** Appends a count variable to the tuple. */
    @ForwardedFields("f0;f1")
    public static final class CountAppender implements MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer, Point, Long> map(Tuple2<Integer, Point> t) {
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
    public static final class CentroidAverager implements MapFunction<Tuple3<Integer, Point, Long>, Tuple5<Integer, Integer, Centroid, Point, COI>> {

        @Override
        public Tuple5<Integer, Integer, Centroid, Point, COI> map(Tuple3<Integer, Point, Long> value) {
            Centroid centroid = new Centroid(value.f0, value.f1.div(value.f2));
            return new Tuple5<>(0, 0, centroid, null, null);
        }

        // DataSet<Tuple5<Integer, Integer, Centroid, Point, COI>>
    }

    /** Assigns each point the cluster 0, which does not exist */
    @ForwardedFields("*->f1")
    public final static class assignPointToNullCluster implements MapFunction<Point, Tuple2<Integer, Point>> {

        @Override
        public Tuple2<Integer, Point> map(Point point) throws Exception {
            return new Tuple2<>(0, point);
        }
    }

    /** Takes all centroids and computes centroid inter-distances */
    public static class computeCentroidInterDistance implements GroupReduceFunction<Centroid, double[][]> {

        @Override
        public void reduce(Iterable<Centroid> iterable, Collector<double[][]> collector) throws Exception {
            List<Centroid> l = new ArrayList<>();

            // Add iterable to List in order to simplify nested loops below
            for(Centroid c : iterable) { l.add(c); }

            // Store the size of the List, which is used in allocation of array below
            int dims = l.size();

            // Ensure that the centroids are sorted in ascending order on their ID
            Collections.sort(l);

            // Allocate the multidimensional array
            double[][] matrix = new double[dims][dims];

            // Computes the distances between every centroid and place them in List l
            for(int i = 0; i < l.size(); i++) {
                Centroid ci = l.get(i);

                for(int j = 0; j < l.size(); j++) {
                    Centroid cj = l.get(j);
                    double dist = ci.euclideanDistance(cj);
                    matrix[i][j] = dist;

                    //System.out.println("Dist between " + i + " & " + j + " is: " + dist);
                }
            }

            // Only used for debugging
            /*for(int i = 0; i < matrix.length; i++) {
                for(int j = 0; j < matrix[i].length; j++) {
                    System.out.print(matrix[i][j] + " ");
                }
                System.out.println("");
            }*/

            collector.collect(matrix);
        }
    }
}
