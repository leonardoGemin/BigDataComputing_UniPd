import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.util.*;

public class G093HW3 {

    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // MAIN PROGRAM
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    public static void main(String[] args) throws Exception {

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CHECKING NUMBER OF CMD LINE PARAMETERS
        // Parameters are: path_to_file, number_of_centers, number_of_allowed_outliers, number_of_partitions
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        if (args.length != 4) {
            throw new IllegalArgumentException("USAGE: file_path num_centers num_allowed_outliers num_partitions");
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SPARK SETUP
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf(true).setAppName("MR k-center with outliers");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");



        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Read position of input file
        String filename = args[0];

        // Read number of centers
        int k = Integer.parseInt(args[1]);

        // Read number of allowed outliers
        int z = Integer.parseInt(args[2]);

        // Read number of partitions
        int L = Integer.parseInt(args[3]);



        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SETTING GLOBAL VARIABLES
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        JavaRDD<Vector> inputPoints;
        long N;

        double objective;
        long start, end;



        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //
        //                - 1 -
        //
        // Read points from file.
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        start = System.currentTimeMillis();
        inputPoints = sc.textFile(args[0], L)
                .map(G093HW3::strToVector)
                .repartition(L)
                .cache();
        N = inputPoints.count();
        end = System.currentTimeMillis();



        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //
        //                - 2 -
        //
        // Print input parameters.
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        System.out.println("File : " + filename);
        System.out.println("Number of points N = " + N);
        System.out.println("Number of centers k = " + k);
        System.out.println("Number of outliers z = " + z);
        System.out.println("Number of partitions L = " + L);
        System.out.println("Time to read from file: " + (end - start) + " ms");



        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //
        //                - 3 -
        //
        // Solve the problem.
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        ArrayList<Vector> solution = MR_kCenterOutliers(inputPoints, k, z, L);



        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //
        //                - 4 -
        //
        // Compute the value of the objective function.
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        start = System.currentTimeMillis();
        objective = computeObjective(inputPoints, solution, z);
        end = System.currentTimeMillis();
        System.out.println("Objective function = " + objective);
        System.out.println("Time to compute objective function: " + (end - start) + " ms");
    }






    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // AUXILIARY METHODS
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&



    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // Method strToVector: input reading
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    public static Vector strToVector(String str) {
        String[] tokens = str.split(",");
        double[] data = new double[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        return Vectors.dense(data);
    }



    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // Method euclidean: distance function
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    public static double euclidean(Vector a, Vector b) {
        return Math.sqrt(Vectors.sqdist(a, b));
    }



    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // Method MR_kCenterOutliers: MR algorithm for k-center with outliers
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    public static ArrayList<Vector> MR_kCenterOutliers(JavaRDD<Vector> points, int k, int z, int L) {
        long start, end, round1, round2;


        //------------- ROUND 1 ---------------------------
        start = System.currentTimeMillis();
        JavaRDD<Tuple2<Vector, Long>> coreset = points
                .mapPartitions(x -> {
                    ArrayList<Vector> partition = new ArrayList<>();
                    while (x.hasNext()) {
                        partition.add(x.next());
                    }

                    ArrayList<Vector> centers = kCenterFFT(partition, k + z + 1);
                    ArrayList<Long> weights = computeWeights(partition, centers);
                    ArrayList<Tuple2<Vector,Long>> c_w = new ArrayList<>();

                    for (int i = 0; i < centers.size(); ++i) {
                        Tuple2<Vector, Long> entry = new Tuple2<>(centers.get(i), weights.get(i));
                        c_w.add(i, entry);
                    }

                    return c_w.iterator();
                }); // END OF ROUND 1
        List<Tuple2<Vector, Long>> coresetCollect = coreset.collect();
        end = System.currentTimeMillis();
        round1 = end - start;


        //------------- ROUND 2 ---------------------------
        start = System.currentTimeMillis();
        //ArrayList<Tuple2<Vector, Long>> elems = new ArrayList<>((k + z + 1) * L);
        //elems.addAll(coreset.collect());
        ArrayList<Tuple2<Vector, Long>> elems = new ArrayList<>(coresetCollect);
        ArrayList<Vector> solution = SeqWeightedOutliers(elems, k, z, 2);// END OF ROUND 2
        end = System.currentTimeMillis();
        round2 = end - start;


        // Print the times
        System.out.println("Time Round 1: " + round1 + " ms");
        System.out.println("Time Round 2: " + round2 + " ms");


        return solution;
    }



    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // Method kCenterFFT: Farthest-First Traversal
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    public static ArrayList<Vector> kCenterFFT(ArrayList<Vector> points, int k) {
        final int n = points.size();
        double[] minDistances = new double[n];
        Arrays.fill(minDistances, Double.POSITIVE_INFINITY);

        ArrayList<Vector> centers = new ArrayList<>(k);

        Vector lastCenter = points.get(0);
        centers.add(lastCenter);

        for (int iter = 1; iter < k; iter++) {
            int maxIdx = 0;
            double maxDist = 0;

            for (int i = 0; i < n; i++) {
                double d = euclidean(points.get(i), lastCenter);
                if (d < minDistances[i]) {
                    minDistances[i] = d;
                }

                if (minDistances[i] > maxDist) {
                    maxDist = minDistances[i];
                    maxIdx = i;
                }
            }

            lastCenter = points.get(maxIdx);
            centers.add(lastCenter);
        }
        return centers;
    }



    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // Method computeWeights: compute weights of coreset points
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    public static ArrayList<Long> computeWeights(ArrayList<Vector> points, ArrayList<Vector> centers) {
        Long[] weights = new Long[centers.size()];
        Arrays.fill(weights, 0L);

        for (int i = 0; i < points.size(); i++) {
            double tmp = euclidean(points.get(i), centers.get(0));
            int myCenter = 0;
            for (int j = 1; j < centers.size(); j++) {
                if (euclidean(points.get(i), centers.get(j)) < tmp) {
                    myCenter = j;
                    tmp = euclidean(points.get(i), centers.get(j));
                }
            }
            weights[myCenter] += 1L;
        }

        return new ArrayList<>(Arrays.asList(weights));
    }



    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // Method SeqWeightedOutliers: sequential k-center with outliers
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    public static ArrayList<Vector> SeqWeightedOutliers(ArrayList<Tuple2<Vector, Long>> P, int k, int z, int alpha) {

        // Pre-computation of all distances:
        //      since distances[][] is a symmetric matrix, distances[i][j] must be equal to distances[j][i].
        //      Moreover, since the distance of a point from itself is zero, distances[i][i] = 0. However,
        //      this instruction is not necessary since the array is already initialized to zero by Java.
        //
        // In the same loops, we compute the minimum distances between first k + z + 1 points
        // and the total weight.

        double[][] distances = new double[P.size()][P.size()];
        long totalWeight = 0L;
        double r = Double.POSITIVE_INFINITY;

        for (int i = 0; i < P.size(); i++) {
            for (int j = i + 1; j < P.size(); j++) {
                distances[i][j] = distances[j][i] = Math.sqrt(Vectors.sqdist(P.get(i)._1, P.get(j)._1));

                if ((i < k + z) & (j < k + z + 1) & (distances[i][j] < r)) {
                    r = distances[i][j];
                }
            }

            totalWeight += P.get(i)._2;
        }

        //--------------------------------------                     //   PSEUDO-CODE:

        r = r / 2;                                                   //   r <-- (min distance between first k + z + 1 points) / 2
        System.out.println("Initial guess = " + r);

        int guesses = 1;

        while (true) {                                               //   while (true) do
            // We have used an array to indicate the points of P which belong or not to the set Z.
            // In particular:
            //   Z[i] = 0  -->  the i-th point of P belongs to Z;
            //   Z[i] = 1  -->  the i-th point of P does not belong to Z.

            int[] Z = new int[P.size()];                             //   |  Z <-- P
            ArrayList<Vector> S = new ArrayList<>();                 //   |  S <-- {}
            long W_Z = totalWeight;                                  //   |  W_Z <-- \sum_{x \in P} w(x)

            while ((S.size() < k) && (W_Z > 0)) {                    //   |  while ((|S| < k) AND (W_Z > 0)) do
                double max = 0;                                      //   |   |  max <-- 0
                int newCenter_index = -1;

                for (int i = 0; i < P.size(); i++) {                 //   |   |  foreach x \in P do
                    long ball_weight = 0;                            //   |   |   |  ball_weight <-- \sum_{y \in B_Z(x, (1+2\alpha)r) w(y)}
                    for (int j = 0; j < P.size(); j++) {
                        // If Z[j] != 0, it means that P does not belong to Z,
                        // so we can skip the j-th iteration.
                        if (Z[j] != 0)
                            continue;

                        if (distances[i][j] <= (1 + 2 * alpha) * r) {
                            ball_weight += P.get(j)._2;
                        }
                    }

                    if (ball_weight > max) {                         //   |   |   |  if (ball_weight > max) then
                        max = ball_weight;                           //   |   |   |   |  max <-- ball_weight;
                        newCenter_index = i;                         //   |   |   +-  +- newCenter <-- x;
                    }
                }

                assert newCenter_index != -1;
                S.add(P.get(newCenter_index)._1);                    //   |   |  S <-- S u {newCenter};

                for (int j = 0; j < P.size(); j++) {                 //   |   |  foreach (y \in B_Z(newCenter, (3 + 4\alpha)r)) do
                    // If Z[j] != 0, it means that P does not belong to Z,
                    // so we can skip the j-th iteration.
                    if (Z[j] != 0)
                        continue;

                    if (distances[newCenter_index][j] <= (3 + 4 * alpha) * r) {
                        Z[j] = 1;                                    //   |   |   |  remove y from Z;
                        W_Z -= P.get(j)._2;                          //   |   +-  +- subtract w(y) from W_Z;
                    }
                }
            }

            if (W_Z <= z) {                                          //   |  if (W_Z \leq z) then
                System.out.println("Final guess = " + r);
                System.out.println("Number of guesses = " + guesses);

                return S;                                            //   |   +- return S;
            }

            r *= 2;                                                  //   +- r <-- 2r;
            guesses++;
        }
    }



    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // Method computeObjective: computes objective function
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    public static double computeObjective(JavaRDD<Vector> points, ArrayList<Vector> centers, int z) {
        // Definition of objective function:
        //    \Phi_{center} (P - Z_S, S) = \max_{x \in P - Z_S} d(x, S)
        // where d(x, S) = \min_{y \in S} d(x, y)


        // Map each point with its minimum distance from any other point in "centers";
        // then sort minDistances by reverse order and take the (z + 1)th value.
        // So we take the element of index z, which is the maximal distance between the minimum distances.

        JavaRDD<Double> minDistances = points
                .map(x -> {
                    double minDist = Double.MAX_VALUE;
                    for (Vector y: centers) {
                        double dist = Math.sqrt(Vectors.sqdist(x, y));
                        if (dist < minDist) {
                            minDist = dist;
                        }
                    }
                    return minDist;
                })
                .sortBy(f -> f, false, points.getNumPartitions());

        return minDistances.take(z + 1).get(z);
    }
}