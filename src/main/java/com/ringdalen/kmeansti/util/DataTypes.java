package com.ringdalen.kmeansti.util;

import java.io.Serializable;

public class DataTypes {
    // *************************************************************************
    //     DATA TYPES
    // *************************************************************************

    /**
     * A n-dimensional point.
     */
    public static class Point implements Serializable {

        public double[] features;
        public int dimension;

        public double upperBound;
        public double[] lowerBound;

        /** A public no-argument constructor is required for POJOs (Plain Old Java Objects) */
        public Point() {}

        /** A public constructor that takes the features, represented as an array of doubles as the argument */
        public Point(double[] features) {
            this.features = features;
            this.dimension = features.length;
        }

        /** Function that adds this point with any given point */
        public Point add(Point other) {
            for(int i = 0; i < dimension; i++) {
                features[i] += other.features[i];
            }

            return this;
        }

        /** Function that divides this point with a given value */
        public Point div(long val) {
            for(int i = 0; i < dimension; i++) {
                features[i] /= val;
            }
            return this;
        }

        /** Function that return the euclidian distance between this point and any given point */
        public double euclideanDistance(Point other) {
            double dist = 0;

            for(int i = 0; i < dimension; i++) {
                dist += Math.pow((features[i] - other.features[i]), 2.0);
            }

            return Math.sqrt(dist);
        }

        /** Function to clear / null-out the point */
        public void clear() {
            for(int i = 0; i < dimension; i++) {
                features[i] = 0.0;
            }
        }

        /** Function to represent the point in a string */
        @Override
        public String toString() {
            StringBuilder s = new StringBuilder();

            for(int i = 0; i < dimension; i++) {
                if (i < dimension-1) {
                    s.append(features[i]).append(" ");
                } else {
                    s.append(features[i]);
                }
            }

            return s.toString();
        }
    }

    /**
     * A n-dimensional centroid, basically a point with an ID.
     */
    public static class Centroid extends Point implements Comparable<Centroid>{

        /** The ID of an centroid, which also represents the cluster */
        public int id;

        /** A public no-argument constructor is required for POJOs (Plain Old Java Objects) */
        public Centroid() {}

        /** A public constructor that takes an id and the features, represented as an array as the arguments */
        public Centroid(int id, double[] features) {
            super(features);
            this.id = id;
        }

        /** A public constructor that takes an id and a Point as the arguments */
        public Centroid(int id, Point p) {
            super(p.features);
            this.id = id;
        }

        public Integer getID() {
            return id;
        }

        /** A method to allow for comparing the ID of two different centroids. Used for sorting */
        public int compareTo(Centroid c) {
            return this.getID().compareTo(c.getID());
        }

        /** Function to represent the point in a string */
        @Override
        public String toString() {
            return id + " " + super.toString();
        }
    }

    /**
     * A class used to store information that will be carried over to the next iteration.
     * COI is an abbreviation of "Carry-Over-Information"
     */
    public static class COI implements Serializable {
        public double[][] iCD;
        public double[] minCD;
        public double[] distMap;

        public COI(double[][] iCD, double[] minCD, double[] distMap) {
            this.iCD = iCD;
            this.minCD = minCD;
            this.distMap = distMap;
        }

        // Print function used for debugging purposes
        @Override
        public String toString() {
            StringBuilder s = new StringBuilder();
            int dimension = minCD.length;

            s.append("\n\nThe matrix is:\n");
            for(int i = 0; i < iCD.length; i++) {
                for(int j = 0; j < iCD[i].length; j++) {
                    s.append(iCD[i][j] + " ");
                }
                s.append("\n");
            }

            s.append("\n\nThe minCD is:\n");
            for(int i = 0; i < dimension; i++) {
                if (i < dimension-1) {
                    s.append(minCD[i]).append(" ");
                } else {
                    s.append(minCD[i]);
                }
            }

            s.append("\n\nThe distMap is:\n");
            for(int i = 0; i < dimension; i++) {
                if (i < dimension-1) {
                    s.append(distMap[i]).append(" ");
                } else {
                    s.append(distMap[i]);
                }
            }

            return s.toString();
        }

    }
}
