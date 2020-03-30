package com.ringdalen.kmeansti.util;

import com.ringdalen.kmeansti.datatype.DataTypes.Centroid;
import com.ringdalen.kmeansti.datatype.DataTypes.Point;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Arrays;

public class Read {
    /**
     * Function to map data from a file to Centroid objects
     */
    public static DataSet<Centroid> CentroidsFromFile(ParameterTool params, ExecutionEnvironment env) {

        DataSet<Centroid> centroids;

        // Parsing d features, plus the ID (thats why the +1 is included) from file to Centroid objects
        centroids = env.readTextFile(params.get("centroids"))
                .map(new ParseCentroidData(params.getInt("d")));

        return centroids;
    }

    /**
     * Function to map data from a file to Point objects
     */
    public static DataSet<Tuple4<Integer, Point, Double, Double[]>> PointsFromFile(ParameterTool params, ExecutionEnvironment env) {

        DataSet<Tuple4<Integer, Point, Double, Double[]>> points;

        // Parsing d features from file to Point objects
        points = env.readTextFile(params.get("points"))
                .map(new ParsePointData(params.getInt("d"), params.getInt("k")));

        return points;
    }

    /** Reads the input data and generate points */
    public static class ParsePointData implements MapFunction<String, Tuple4<Integer, Point, Double, Double[]>> {
        double[] row;
        int k;

        public ParsePointData(int d, int k){
            this.row = new double[d];
            this.k = k;
        }

        @Override
        public Tuple4<Integer, Point, Double, Double[]> map(String s) {
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
    public static class ParseCentroidData implements MapFunction<String, Centroid> {
        double[] row;

        public ParseCentroidData(int d){
            row = new double[d];
        }

        @Override
        public Centroid map(String s) {
            String[] buffer = s.split(" ");
            int id = Integer.parseInt(buffer[0]);

            // buffer is +1 since this array is one longer
            for(int i = 0; i < row.length; i++) {
                row[i] = Double.parseDouble(buffer[i+1]);
            }

            return new Centroid(id, row);
        }
    }
}
