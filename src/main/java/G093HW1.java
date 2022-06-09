import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class G093HW1 {

    public static void main(String[] args) throws IOException {

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CHECKING NUMBER OF CMD LINE PARAMETERS
        // Parameters are: num_partitions, high_pop_product, country, <path_to_file>
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        if (args.length != 4) {
            throw new IllegalArgumentException("USAGE: num_partitions high_pop_product country file_path");
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SPARK SETUP
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        SparkConf conf = new SparkConf(true).setAppName("Group093_HomeWork1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");



        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Read number of partitions
        int K = Integer.parseInt(args[0]);

        // Read number of the H products with highest popularity
        int H = Integer.parseInt(args[1]);

        // Read country
        String S = args[2];

        // Read position of input file
        String inputFile = args[3];



        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SETTING GLOBAL VARIABLES
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        JavaRDD<String> rawData;
        JavaPairRDD<String, Integer> productCustomer;
        JavaPairRDD<String, Integer> productPopularity1;
        JavaPairRDD<String, Integer> productPopularity2;

        long numTransactions;
        long numProductCustomerPairs;



        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //
        //                - 1 -
        //
        // Reads the input file into an RDD of strings called rawData
        // (each 8-field row is read as a single string), and subdivides it
        // into K partitions, and prints the number of rows read from the input file
        // (i.e., the number of elements of the RDD).
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Read input file and subdivide it into K random partitions
        rawData = sc.textFile(inputFile).repartition(K).cache();

        // Print the number of rows
        numTransactions = rawData.count();
        System.out.println("Number of rows = " + numTransactions);



        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //
        //                - 2 -
        //
        // Transforms rawData into an RDD of (String, Integer) pairs called productCustomer,
        // which contains all distinct pairs (P, C) such that rawData contains one or more strings
        // whose constituent fields satisfy the following conditions:
        //   - ProductID=P and CustomerID=C, Quantity>0, and Country=S.
        //
        // If S="all", no condition on Country is applied. It then prints the number of pairs in the RDD.
        //
        // IMPORTANT: since the dataset can be potentially very large, the rows relative to a given product P
        // might be too many and you must not gather them together; however, you can safely assume that
        // the rows relative to a given product P and a given customer C are not many (say constant number).
        //
        // Also, although the RDD interface offers a method distinct() to remove duplicates,
        // we ask you to avoid using this method for this step.
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        /*
         * ALTERNATIVE ALGORITHM
         * WITH   R = O(1),
         *      M_L = O(sqrt(N)),
         *  AND M_A = O(1)
         *

        Random randomGenerator = new Random();
        int N = (int) Math.sqrt(numTransactions);

        productCustomer = rawData
                // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                // Round 1: MAP PHASE
                //
                // rawData RDD  -->  (R, (productID, customerID))
                // where R is a random number in [0, sqrt(N))
                //
                // M_A = O(N)
                // M_L = O(1)
                // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                .flatMapToPair((document) -> {
                    String[] tokens = document.split(",");
                    ArrayList<Tuple2<Integer, Tuple2<String, Integer>>> pairs = new ArrayList<>();

                    // quantity > 0  AND  (country = S  OR  S = "all")
                    if ((Integer.parseInt(tokens[3]) > 0) && ((tokens[7].equals(S)) || (S.equals("all")))) {
                        pairs.add(new Tuple2<>(randomGenerator.nextInt(N), new Tuple2<>(tokens[1], Integer.parseInt(tokens[6]))));
                    }

                    return pairs.iterator();
                })

                // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                // Round 1: SHUFFLE + GROUPING
                // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                .groupByKey()

                // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                // Round 1: REDUCE PHASE
                //
                // for each j in [0, sqrt(N)) separately, let L_j be the list of pairs (productID, customerID)
                // from intermediate pairs (j, (productID, customerID))
                //
                // (j, L_j)  -->  {(j, (productID, customerID)) for each distinct pairs (productID, customerID) in L_j}
                //
                // M_A = O(N)
                // M_L = O(sqrt(N))
                // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                .flatMapToPair((element) -> {
                    Set <Tuple2<String, Integer>> uniquePairs = new HashSet<>();
                    for (Tuple2<String, Integer> e : element._2()) {
                        uniquePairs.add(e);
                    }

                    ArrayList<Tuple2<Integer, Tuple2<String, Integer>>> pairs = new ArrayList<>();
                    for (Tuple2<String, Integer> e : uniquePairs) {
                        pairs.add(new Tuple2<>(element._1(), e));
                    }
                    return pairs.iterator();
                })


                // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                // Round 2: MAP PHASE
                //
                // for each pairs (j, (productID, customerID)) from ROUND 1,
                // (j, (productID, customerID))  -->  ((productID, customerID), j)
                //
                // M_A = O(N)
                // M_L = O(1)
                // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                .mapToPair(Tuple2::swap)

                // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                // Round 2: SHUFFLE + GROUPING
                // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                .groupByKey()

                // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                // Round 2: REDUCE PHASE
                //
                // for each pair (productID, customerID), let L_(productID, customerID) be the list of indices j
                // of intermediate pairs.
                //
                // ((productID, customerID), L_(productID, customerID))  -->  (productID, customerID)
                //
                // M_A = O(N)
                // M_L = O(sqrt(N))
                // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                .flatMapToPair((element) -> {
                    ArrayList<Tuple2<String, Integer>> pairs = new ArrayList<>();
                    pairs.add(element._1());
                    return pairs.iterator();
                });
         */

        productCustomer = rawData
                // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                // Round 1: MAP PHASE
                //
                // rawData RDD  -->  ((productID, customerID), 1)
                // where "1" is a dummy value
                // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                .flatMapToPair((document) -> {
                    String[] tokens = document.split(",");
                    ArrayList<Tuple2<Tuple2<String, Integer>, Integer>> pairs = new ArrayList<>();

                    // quantity > 0  AND  (country = S  OR  S = "all")
                    if ((Integer.parseInt(tokens[3]) > 0) && ((tokens[7].equals(S)) || (S.equals("all")))) {
                        pairs.add(new Tuple2<>(new Tuple2<>(tokens[1], Integer.parseInt(tokens[6])), 1));
                    }

                    return pairs.iterator();
                })

                // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                // Round 1: SHUFFLE + GROUPING
                // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                .groupByKey()

                // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                // Round 1: REDUCE PHASE
                //
                // for each pair (productID, customerID), let L_(productID, customerID) be the list of indices j
                // of intermediate pairs.
                //
                // ((productID, customerID), L_(productID, customerID))  -->  (productID, customerID)
                // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                .flatMapToPair((element) -> {
                    ArrayList<Tuple2<String, Integer>> pairs = new ArrayList<>();
                    pairs.add(element._1());
                    return pairs.iterator();
                });

        numProductCustomerPairs = productCustomer.count();
        System.out.println("Product-Customer Pairs = " + numProductCustomerPairs);



        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //
        //                - 3 -
        //
        // Uses the mapPartitionsToPair/mapPartitions method to transform productCustomer
        // into an RDD of (String, Integer) pairs called productPopularity1 which,
        // for each product ProductID contains one pair (ProductID, Popularity),
        // where Popularity is the number of distinct customers from Country S (or from all countries if S="all")
        // that purchased a positive quantity of product ProductID.
        //
        // IMPORTANT: in this case it is safe to assume that the amount of data in a partition is small enough
        // to be gathered together.
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        productPopularity1 = productCustomer
                // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                // Round 1: MAP PHASE
                //
                // for each pairs (productID, customerID) from productCustomer RDD,
                // (productID, customerID)  -->  (productID, 1)
                //
                // M_A = O(N)
                // M_L = O(1)
                // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                // Round 1: REDUCE PHASE
                //
                // for each productID separately, let L_productID be the list of customerID from
                // the intermediate pairs.
                //
                // (productID, L_productID)  -->  (productID, |L_productID|)
                //
                // M_A = O(N)
                // M_L = O(sqrt(N))
                // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                .mapPartitionsToPair((element) -> {
                    HashMap<String, Integer> counts = new HashMap<>();
                    while (element.hasNext()){
                        Tuple2<String, Integer> tuple = element.next();
                        counts.put(tuple._1(), 1 + counts.getOrDefault(tuple._1(), 0));
                    }

                    ArrayList<Tuple2<String, Integer>> pairs = new ArrayList<>();
                    for (Map.Entry<String, Integer> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })
                .groupByKey()
                .mapValues((element) -> {
                    int sum = 0;
                    for (int value : element) {
                        sum += value;
                    }
                    return sum;
                });



        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //
        //                - 4 -
        //
        // Repeats the operation of the previous point using a combination of map/mapToPair
        // and reduceByKey methods (instead of mapPartitionsToPair/mapPartitions) and calling
        // the resulting RDD productPopularity2.
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        productPopularity2 = productCustomer
                // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                // Round 1: MAP PHASE
                //
                // for each pairs (productID, customerID) from productCustomer RDD,
                // (productID, customerID)  -->  (productID, 1)
                //
                // M_A = O(N)
                // M_L = O(1)
                // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                .mapToPair((element) -> new Tuple2<>(element._1(), 1))

                // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                // Round 1: REDUCE PHASE
                //
                // for each productID separately, let L_productID be the list of 1's from
                // the intermediate pairs.
                //
                // (productID, L_productID)  -->  (productID, \sum_{1 \in L_productID} 1)
                //
                // M_A = O(N)
                // M_L = O(sqrt(N))
                // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&
                .reduceByKey(Integer::sum);



        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //
        //                - 5 -
        //
        // (This step is executed only if H>0)
        //
        // Saves in a list and prints the ProductID and Popularity of the H products with highest Popularity.
        // Extracts these data from productPopularity1.
        // Since the RDD can be potentially very large you must not spill the entire RDD onto a list
        // and then extract the top-H products.
        //
        // Check the guide Introduction to Programming in Spark to find useful methods to efficiently extract
        // top-valued elements from an RDD.
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        if (H != 0) {
            List<Tuple2<String, Integer>> top_H_Popularity;

            top_H_Popularity = productPopularity1
                    .mapToPair(Tuple2::swap)
                    .sortByKey(false)
                    .mapToPair(Tuple2::swap)
                    .take(H);

            System.out.println("Top " + H + " Products and their Popularities");
            for (Tuple2<String, Integer> e : top_H_Popularity) {
                System.out.print("Product " + e._1 + " Popularity " + e._2 + "; ");
            }
        }



        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //
        //                - 6 -
        //
        // (This step, for debug purposes, is executed only if H=0)
        //
        // Collects all pairs of productPopularity1 into a list and print all of them,
        // in increasing lexicographic order of ProductID.
        // Repeats the same thing using productPopularity2.
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        if (H == 0) {
            List<Tuple2<String, Integer>> orderedProductPopularity1;
            List<Tuple2<String, Integer>> orderedProductPopularity2;

            orderedProductPopularity1 = productPopularity1
                    .sortByKey(true)
                    .collect();

            orderedProductPopularity2 = productPopularity2
                    .sortByKey(true)
                    .collect();

            System.out.println("productPopularity1:");
            for (Tuple2<String, Integer> e : orderedProductPopularity1) {
                System.out.print("Product: " + e._1 + " Popularity: " + e._2 + "; ");
            }

            System.out.print("\n");

            System.out.println("productPopularity2:");
            for (Tuple2<String, Integer> e : orderedProductPopularity2) {
                System.out.print("Product: " + e._1 + " Popularity: " + e._2 + "; ");
            }
        }
    }
}