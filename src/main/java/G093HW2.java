import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class G093HW2 {

    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // INPUT READING METHODS
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    public static Vector strToVector(String str) {
        String[] tokens = str.split(",");
        double[] data = new double[tokens.length];
        for (int i=0; i<tokens.length; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        return Vectors.dense(data);
    }

    public static ArrayList<Vector> readVectorsSeq(String filename) throws IOException {
        if (Files.isDirectory(Paths.get(filename))) {
            throw new IllegalArgumentException("readVectorsSeq is meant to read a single file.");
        }
        ArrayList<Vector> result = new ArrayList<>();
        Files.lines(Paths.get(filename))
                .map(G093HW2::strToVector)
                .forEach(result::add);
        return result;
    }



    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // SeqWeightedOutliers METHOD
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    public static ArrayList<Vector> SeqWeightedOutliers(ArrayList<Vector> P, ArrayList<Long> W, int k, int z, int alpha) {

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
            distances[i][i] = 0;
            for (int j = i + 1; j < P.size(); j++) {
                distances[i][j] = distances[j][i] = Math.sqrt(Vectors.sqdist(P.get(i), P.get(j)));

                if ((i < k + z) & (j < k + z + 1) & (distances[i][j] < r)) {
                    r = distances[i][j];
                }
            }

            totalWeight += W.get(i);
        }

        //--------------------------------------                     //   PSEUDO-CODE:

        r = r / 2;                                                   //   r <-- (min distance between first k + z + 1 points) / 2
        System.out.println("Initial guess = " + r);

        int guesses = 1;

        while (true) {                                               //   while (true) do
            // Z is an array of integers initialized to 0.
            // We have used this array to indicate the points of P which belong or not to the set Z.
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
                            ball_weight += W.get(j);
                        }
                    }

                    if (ball_weight > max) {                         //   |   |   |  if (ball_weight > max) then
                        max = ball_weight;                           //   |   |   |   |  max <-- ball_weight;
                        newCenter_index = i;                         //   |   |   +-  +- newCenter <-- x;
                    }
                }

                assert newCenter_index != -1;
                S.add(P.get(newCenter_index));                       //   |   |  S <-- S u {newCenter};

                for (int j = 0; j < P.size(); j++) {                 //   |   |  foreach (y \in B_Z(newCenter, (3 + 4\alpha)r)) do
                    // If Z[j] != 0, it means that P does not belong to Z,
                    // so we can skip the j-th iteration.
                    if (Z[j] != 0)
                        continue;

                    if (distances[newCenter_index][j] <= (3 + 4 * alpha) * r) {
                        Z[j] = 1;                                    //   |   |   |  remove y from Z;
                        W_Z -= W.get(j);                             //   |   +-  +- subtract w(y) from W_Z;
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



    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // ComputeObjective METHOD
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    public static double ComputeObjective(ArrayList<Vector> P, ArrayList<Vector> S, int z) {
        // \Phi_{center} (P - Z_S, S) = \max_{x \in P - Z_S} d(x, S)

        ArrayList<Double> minDistances = new ArrayList<>();
        for (Vector x: P) {
            // minDistances[] = minDist
            // minDist = d(x, S) = \min_{y \in S} d(x, y)

            double minDist = Double.MAX_VALUE;
            for (Vector y: S) {
                double dist = Math.sqrt(Vectors.sqdist(x, y));
                if (dist < minDist) {
                    minDist = dist;
                }
            }

            minDistances.add(minDist);
        }

        // Sort minDistances by reverse order. Then take the (z + 1)th value, so the element with index z,
        // that is the maximal distance ....
        minDistances.sort(Collections.reverseOrder());
        return minDistances.get(z);
    }



    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // MAIN METHOD
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    public static void main(String[] args) throws IOException {

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CHECKING NUMBER OF CMD LINE PARAMETERS
        // Parameters are: path_to_file, number_of_centers, number_of_allowed_outliers
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        if (args.length != 3) {
            throw new IllegalArgumentException("USAGE: file_path num_centers num_allowed_outliers");
        }



        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Read position of input file
        String inputFile = args[0];

        // Read number of centers
        int k = Integer.parseInt(args[1]);

        // Read number of allowed outliers
        int z = Integer.parseInt(args[2]);



        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SETTING GLOBAL VARIABLES
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        ArrayList<Vector> inputPoints;
        ArrayList<Long> weights;
        ArrayList<Vector> solution;

        int n;
        double objective;
        long SeqWeightedOutliersTime;
        long start, end;



        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //
        //                - 1 -
        //
        // Read the points in the input file into an ArrayList<Vector>
        // called inputPoints.
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Read input file
        inputPoints = readVectorsSeq(inputFile);
        n = inputPoints.size();

        System.out.println("Input size n = " + n);
        System.out.println("Number of centers k = " + k);
        System.out.println("Number of outliers z = " + z);



        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //
        //                - 2 -
        //
        // Create an ArrayList<Long> called weights of the same cardinality of inputPoints,
        // initialized with all 1's.
        //
        // In this homework we will use unit weights, but in Homework 3 we will need
        // the generality of arbitrary integer weights!.
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        weights = new ArrayList<>(Collections.nCopies(n, 1L));



        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //
        //                - 3 -
        //
        // Run SeqWeightedOutliers(inputPoints, weights, k, z, 0) to compute a set of (at most) k centers.
        // The output of the method must be saved into an ArrayList<Vector> called solution.
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        start = System.currentTimeMillis();
        solution = SeqWeightedOutliers(inputPoints, weights, k, z, 0);
        end = System.currentTimeMillis();

        SeqWeightedOutliersTime = end - start;



        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //
        //                - 4 -
        //
        // Run ComputeObjective(inputPoints, solution, z) and save the output in a variable called objective.
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        objective = ComputeObjective(inputPoints, solution, z);



        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //
        //                - 5 -
        //
        // Return as output the following quantities:
        //   - n = |P|,
        //   - k,
        //   - z,
        //   - the initial value of the guess r,
        //   - the final value of the guess r,
        //   - and the number of guesses made by SeqWeightedOutliers(inputPoints, weights, k, z, 0),
        //   - the value of the objective function (variable objective),
        //   - and the time (in milliseconds) required by the execution of
        //         SeqWeightedOutliers(inputPoints, weights, k, z, 0).
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        System.out.println("Objective function = " + objective);
        System.out.println("Time of SeqWeightedOutliers = " + SeqWeightedOutliersTime);
    }
}