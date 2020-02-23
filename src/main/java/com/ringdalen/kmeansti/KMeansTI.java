package com.ringdalen.kmeansti;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;
import java.util.Collection;

/**
 * This example implements a basic K-Means clustering algorithm.
 *
 * <p>K-Means is an iterative clustering algorithm and works as follows:<br>
 * K-Means is given a set of data points to be clustered and an initial set of <i>K</i> cluster centers.
 * In each iteration, the algorithm computes the distance of each data point to each cluster center.
 * Each point is assigned to the cluster center which is closest to it.
 * Subsequently, each cluster center is moved to the center (<i>mean</i>) of all points that have been assigned to it.
 * The moved cluster centers are fed into the next iteration.
 * The algorithm terminates after a fixed number of iterations (as in this implementation)
 * or if cluster centers do not (significantly) move in an iteration.<br>
 * This is the Wikipedia entry for the <a href="http://en.wikipedia.org/wiki/K-means_clustering">K-Means Clustering algorithm</a>.
 *
 * <p>This implementation works on two-dimensional data points. <br>
 * It computes an assignment of data points to cluster centers, i.e.,
 * each data point is annotated with the id of the final cluster (center) it belongs to.
 *
 * <p>Input files are plain text files and must be formatted as follows:
 * <ul>
 * <li>Data points are represented as two double values separated by a blank character.
 * Data points are separated by newline characters.<br>
 * For example <code>"1.2 2.3\n5.3 7.2\n"</code> gives two data points (x=1.2, y=2.3) and (x=5.3, y=7.2).
 * <li>Cluster centers are represented by an integer id and a point value.<br>
 * For example <code>"1 6.2 3.2\n2 2.9 5.7\n"</code> gives two centers (id=1, x=6.2, y=3.2) and (id=2, x=2.9, y=5.7).
 * </ul>
 *
 * <p>Usage: <code>KMeans --points &lt;path&gt; --centroids &lt;path&gt; --output &lt;path&gt; <br>--iterations &lt;n&gt; --d &lt;n dimensions&gt;</code><br>
 *
 * <p>This example shows how to use:
 * <ul>
 * <li>Bulk iterations
 * <li>Broadcast variables in bulk iterations
 * <li>Custom Java objects (POJOs)
 * </ul>
 */

@SuppressWarnings("serial")
public class KMeansTI {

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data:
        // read the points and centroids from the provided paths or fall back to default data
        DataSet<Point> points = getPointDataSet(params, env);
        DataSet<Centroid> centroids = getCentroidDataSet(params, env);

        // set number of bulk iterations for KMeans algorithm
        IterativeDataSet<Centroid> loop = centroids.iterate(params.getInt("iterations", 10));

        DataSet<Centroid> newCentroids = points
                // compute closest centroid for each point
                .map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")

                // count and sum point coordinates for each centroid
                .map(new CountAppender())
                .groupBy(0).reduce(new CentroidAccumulator())

                // compute new centroids from point counts and coordinate sums
                .map(new CentroidAverager());

        // feed new centroids back into next iteration
        DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);

        DataSet<Tuple2<Integer, Point>> clusteredPoints = points
                // assign points to final clusters
                .map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

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

        // Parsing all the data from file to Centroid objects
        centroids = env.readTextFile(params.get("centroids"))
                .map(new ReadCentroidData(params.getInt("d")));

        return centroids;
    }

    /**
     * Function to map data from a file to Point objects
     */
    private static DataSet<Point> getPointDataSet(ParameterTool params, ExecutionEnvironment env) {

        DataSet<Point> points;

        // Parsing all the data from file to Point objects
        points = env.readTextFile(params.get("points"))
                .map(new ReadPointData(params.getInt("d")));

        return points;
    }

    // *************************************************************************
    //     DATA TYPES
    // *************************************************************************

    /**
     * A n-dimensional point.
     */
    public static class Point implements Serializable {

        // public double x, y;

        public double[] features;
        public int dimension;

        /** A public no-argument constructor is required for POJOs (Plain Old Java Objects) */
        public Point() {}

        /*public Point(double x, double y) {
            this.x = x;
            this.y = y;
        }*/

        /** A point consists of n features */
        public Point(double[] features) {
            this.features = features;
            this.dimension = features.length;
        }

        /*public Point add(Point other) {
            x += other.x;
            y += other.y;
            return this;
        } */

        /** Function that adds this point with any given point */
        public Point add(Point other) {
            for(int i = 0; i < dimension; i++) {
                features[i] += other.features[i];
            }

            return this;
        }

        /*public Point div(long val) {
            x /= val;
            y /= val;
            return this;
        } */

        /** Function that divides this point with a given value */
        public Point div(long val) {
            for(int i = 0; i < dimension; i++) {
                features[i] /= val;
            }
            return this;
        }

        /*public double euclideanDistance(Point other) {
            return Math.sqrt((x - other.x) * (x - other.x) + (y - other.y) * (y - other.y));
        } */

        /** Function that return the euclidian distance between this point and any given point */
        public double euclideanDistance(Point other) {
            double dist = 0;

            for(int i = 0; i < dimension; i++) {
                dist += Math.pow((features[i] - other.features[i]), 2.0);
            }

            return Math.sqrt(dist);
        }

        /* public void clear() {
            x = y = 0.0;
        } */

        public void clear() {
            for(int i = 0; i < dimension; i++) {
                features[i] = 0.0;
            }
        }

        /*@Override
        public String toString() {
            return x + " " + y;
        } */

        /** Function to represent the point in a string */
        @Override
        public String toString() {
            StringBuilder s = new StringBuilder();

            for(int i = 0; i < dimension; i++) {
                s.append(features[i]).append(" ");
            }

            return s.toString();
        }
    }

    /**
     * A n-dimensional centroid, basically a point with an ID.
     */
    public static class Centroid extends Point {

        public int id;

        public Centroid() {}

        /*public Centroid(int id, double x, double y) {
            super(x, y);
            this.id = id;
        }*/

        public Centroid(int id, double[] features) {
            super(features);
            this.id = id;
        }

        /*public Centroid(int id, Point p) {
            super(p.x, p.y);
            this.id = id;
        }*/

        public Centroid(int id, Point p) {
            super(p.features);
            this.id = id;
        }

        @Override
        public String toString() {
            return id + ": " + super.toString();
        }
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /** Function to take the input data and generate points */
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

    /** Function to take the input data and generate centroids */
    public static class ReadCentroidData implements MapFunction<String, Centroid> {
        double[] row;

        public ReadCentroidData(int d){
            row = new double[d];
        }

        @Override
        public Centroid map(String s) throws Exception {
            String[] buffer = s.split(" ");
            int id = Integer.parseInt(buffer[0]);

            // Start loop at one since first number is ID of centroid
            for(int i = 1; i < row.length; i++) {
                row[i] = Double.parseDouble(buffer[i]);
            }

            return new Centroid(id, row);
        }
    }

    /** Determines the closest cluster center for a data point. */
    @ForwardedFields("*->1")
    public static final class SelectNearestCenter extends RichMapFunction<Point, Tuple2<Integer, Point>> {
        private Collection<Centroid> centroids;

        /** Reads the centroid values from a broadcast variable into a collection. */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public Tuple2<Integer, Point> map(Point p) throws Exception {

            double minDistance = Double.MAX_VALUE;
            int closestCentroidId = -1;

            // check all cluster centers
            for (Centroid centroid : centroids) {
                // compute distance
                double distance = p.euclideanDistance(centroid);

                System.out.println("AHEEM! Calculated distance between " + p.toString() + " and " + centroid.toString() + " is " + distance);

                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = centroid.id;
                }
            }

            // emit a new record with the center id and the data point.
            return new Tuple2<>(closestCentroidId, p);
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
}
