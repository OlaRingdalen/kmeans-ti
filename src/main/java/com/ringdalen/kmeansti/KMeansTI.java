package com.ringdalen.kmeansti;

import com.ringdalen.kmeansti.util.DataTypes.Point;
import com.ringdalen.kmeansti.util.DataTypes.Centroid;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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
        DataSet<Point> points = getPointDataSet(params, env);
        DataSet<Centroid> centroids = getCentroidDataSet(params, env);

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

        DataSet<Tuple2<Integer, Point>> nullClusteredPoints = points
                .map(new assignPointToNullCluster());

        // Loop begins here
        IterativeDataSet<Centroid> loop = centroids.iterate(iterations);

        // Asssigning each point to the nearest centroid
        DataSet<Tuple2<Integer, Point>> partialClusteredPoints = nullClusteredPoints
                // Compute closest centroid for each point
                .map(new SelectNearestCenter())
                .withBroadcastSet(loop, "centroids");

        // Producing new centroids based on the clustered points
        DataSet<Centroid> newCentroids = partialClusteredPoints
                // Count and sum point coordinates for each centroid
                .map(new CountAppender())
                .groupBy(0).reduce(new CentroidAccumulator())
                // Compute new centroids from point counts and coordinate sums
                .map(new CentroidAverager());

        // Loop ends here. Feed new centroids back into next iteration
        DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);

        DataSet<Tuple2<Integer, Point>> clusteredPoints = nullClusteredPoints
                // assign points to final clusters
                .map(new SelectNearestCenter())
                .withBroadcastSet(finalCentroids, "centroids");


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
    private static DataSet<Point> getPointDataSet(ParameterTool params, ExecutionEnvironment env) {

        DataSet<Point> points;

        // Parsing d features from file to Point objects
        points = env.readTextFile(params.get("points"))
                .map(new ReadPointData(params.getInt("d")));

        return points;
    }

    /**
     * Function to map partial clustered points to a Tuple2
     */
    private static DataSet<Tuple2<Integer, Point>> getPartialClusteredPoints(String filePath, int d, ExecutionEnvironment env) {

        DataSet<Tuple2<Integer, Point>> partialClusteredPoints;

        // Parsing d features from file to Point objects
        partialClusteredPoints = env.readTextFile(filePath)
                .map(new ReadPartialClusteredPoints(d));

        return partialClusteredPoints;
    }



    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /** Reads the input data and generate points */
    public static class ReadPointData implements MapFunction<String, Point> {
        double[] row;

        public ReadPointData(int d){
            row = new double[d];
        }

        @Override
        public Point map(String s) throws Exception {
            String[] buffer = s.split(" ");

            for(int i = 0; i < row.length; i++) {
                row[i] = Double.parseDouble(buffer[i]);
            }

            return new Point(row);
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

    /** Reads the partial clustered points and and return a Tuple2 with the ID and point */
    public static class ReadPartialClusteredPoints implements MapFunction<String, Tuple2<Integer, Point>> {
        double[] row;

        public ReadPartialClusteredPoints(int d){
            row = new double[d];
        }

        @Override
        public Tuple2<Integer, Point> map(String s) throws Exception {
            String[] buffer = s.split(" ");
            int id = Integer.parseInt(buffer[0]);

            // buffer is +1 since this array is one longer
            for(int i = 0; i < row.length; i++) {
                row[i] = Double.parseDouble(buffer[i+1]);
            }

            LOG.error("Reading from disk!");

            return new Tuple2<>(id, new Point(row));
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
    @ForwardedFields("0->id")
    public static final class CentroidAverager implements MapFunction<Tuple3<Integer, Point, Long>, Centroid> {

        @Override
        public Centroid map(Tuple3<Integer, Point, Long> value) {
            return new Centroid(value.f0, value.f1.div(value.f2));
        }
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
