# Big Data Computing
MSc homework experiences at UNIPD.
<br />

## Homework 1
The purpose of this first homework is to get acquainted with Spark and with its use to implement MapReduce algorithms. In preparation for the homework, perform the following **preliminary steps**:
- Set up your environment following the instructions given in the dedicated section here in Moodle Exam.
  
- Download the WordCountExample program (Java or Python), run it and get acquainted with the Spark methods it uses. (The [Introduction to Programming in Spark](http://www.dei.unipd.it/~capri/BDC/SparkProgramming.html) page may turn out useful to this purpose.)
  
You will develop a Spark program to analyze a dataset of an online retailer which contains several transactions made by customers,  where a transaction represents several products purchased by a customer. Your program must be designed for very large datasets.

- **DATA FORMAT**. The dataset is provided as a file where each row is associated to a product purchased in a transaction. More specifically, a row consists of 8 comma-separated fields: _TransactionID_ (a string uniquely representing a transaction), _ProductID_ (a string uniquely representing a product), _Description_ (a string describing the product), _Quantity_ (an integer representing the units of product purchased), _InvoiceDate_ (the date of the transaction), _UnitPrice_ (a real representing the price of a unit of product), _CustomerID_ (an integer uniquely representing a customer), _Country_ (a string representing the country of the customer). For example the row: **A536370, 21883B, STAR GIFT TAPE, 24, 2010-12-01 8:45, 0.65, 12583, France** represents the fact that Transaction A536370 made by Customer 12583 on 12/1/2010 at 8.45am contains 24 units of Product 21883B called Star Gift Tape. If in the same transaction the customer purchased several products, they will be represented in separate rows. Note that the Quantity field can also contain a negative value -x to denote that x previously purchased units of the product have been returned. 
  
- **TASK**: you must write a program **GxxxHW1.java** (for Java users) or **GxxxHW1.py** (for Python users), where xxx is your 3-digit group number (e.g., 004 or 045), which receives in input, as command-line (CLI) arguments, 2 integers **K** and **H**, a string **S**, and a path to the file storing the dataset, and does the following:
  1. Reads the input file into an **RDD of strings** called **rawData** (each 8-field row is read as a single string), and subdivides it into **K partitions**, and prints the number of rows read from the input file (i.e., the number of elements of the RDD). 
    
  2. Transforms rawData into an **RDD of (String,Integer) pairs** called **productCustomer**, which contains all distinct pairs (P,C) such that rawData contains one or more strings whose constituent fields satisfy the following conditions : ProductID=P and CustomerID=C, Quantity>0, and Country=S. If S="all", no condition on Country is applied. It then prints the number of pairs in the RDD. **IMPORTANT**: _since the dataset can be potentially very large, the rows relative to a given product P might be too many and you must not gather them together; however, you can safely assume that the rows relative to a given product P and a given customer C are not many (say constant number). Also, although the RDD interface offers a method distinct() to remove duplicates, we ask you to avoid using this method for this step_. 
    
  3. Uses the mapPartitionsToPair/mapPartitions method (combined with the groupByKey and mapValues or mapToPair/map methods) to transform productCustomer into an **RDD of (String,Integer) pairs** called **productPopularity1** which, for each product ProductID contains one pair (ProductID, Popularity), where Popularity is the number of distinct customers from Country S (or from all countries if S="all") that purchased a positive quantity of product ProductID. **IMPORTANT**: _in this case it is safe to assume that the amount of data in a partition is small enough to be gathered together_. 
    
  4. Repeats the operation of the previous point using a combination of map/mapToPair and reduceByKey methods (instead of mapPartitionsToPair/mapPartitions) and calling the resulting RDD **productPopularity2**.
    
  5. (This step is executed only if H>0) Saves in a list and prints the ProductID and Popularity of the **H products with highest Popularity**. Extracts these data from **productPopularity1**. Since the RDD can be potentially very large _you must not spill the entire RDD onto a list and then extract the top-H products. Check the guide [Introduction to Programming in Spark](http://www.dei.unipd.it/~capri/BDC/SparkProgramming.html) to find useful methods to efficiently extract top-valued elements from an RDD_.
    
  6. (This step, for debug purposes, is executed only if H=0) Collects all pairs of **productPopularity1** into a list and print all of them, in increasing lexicographic order of ProductID. Repeats the same thing using **productPopularity2**. 
  
To test your program you can use the 3 datasets we provide, in the same section as this specification, together with the outputs of some runs of the program on the datasets.<br /><br />
<br />

## Homework 2
**INTRODUCTION**. Homeworks 2 and 3 will focus on the **k-center with z outliers problem**, that is, the robust version of the k-center problem which is useful in the analysis of noisy data (a quite common scenario in big data computing). Given a set P of points and two integers k and z, the problem requires to determine a set S ⊂ P of k centers which minimize the maximum distance of a point of P-Z from S, where Z are the z farthest points of P from S. In other words, with respect to the standard k-center problem, in the k-center with z outliers problem, we are allowed to disregard the z farthest points from S in the objective function. Unfortunately, the solution of this problem turns out much harder than the one of the standard k-center problem. The 3-approximation sequential algorithm by Charikar et al. for k-center with z outliers, which we call **kcenterOUT**, is simple to implement but has superlinear complexity (more than quadratic, unless sophisticated data structures are used). The rigorous specification of the problem and the description of kcenterOUT with pseudocode, can be found in this set of slides: [PresentationHW2.pdf](#).

The two homeworks will demonstrate that in this case a coreset-based approach can be successfully employed. In Homework 2 you will implement the 3-approximation sequential algorithm and will get a first-hand experience of its inefficiency. In Homework 3, you will implement a 2-round MapReduce coreset-based algorithm for the problem, where the use of the inefficient 3-approximation is confined to a small coreset computed in parallel through the efficient Farthest-First Traversal.

**REPRESENTATION of POINTS**. We will work with points in Euclidean space (real cooordinates) and with the Euclidean L2-distance.

FOR JAVA USERS. In Spark, points can be represented as instances of the class _org.apache.spark.mllib.linalg.Vector_ and can be manipulated through static methods offered by the class _org.apache.spark.mllib.linalg.Vectors_. For example, method Vectors.dense(x) transforms an array x of double into an instance of class Vector.  Given two points x and y, instances of Vector, their Euclidean L2-distance can be computed by invoking: Math.sqrt(Vectors.sqdist(x, y)). Details on the classes can be found in the [Spark Java API](https://spark.apache.org/docs/latest/api/java/index.html).  **Warning**. _Make sure to use the classes from the org.apache.spark.mllib package. There are classes with the same name in org.apache.spark.ml package which are functionally equivalent, but incompatible with those of the org.apache.spark.mllib package_.

FOR PYTHON USERS. We suggest to represent points as the standard _tuple of float_ (i.e., point = (x1, x2, ...)). Although Spark provides the class Vector also for Python (see pyspark.mllib package), its performance is very poor and its more convenient to use tuples, especially for points from low-dimensional spaces.

**TASK for HOMEWORK 2**: you must
1. **Develop a method SeqWeightedOutliers(P,W,k,z,alpha) which implements the weighted variant of kcenterOUT** (the 3-approximation algorithm for k-center with z-outliers). The method takes as input the set of points P, the set of weights W, the number of centers k, the number of outliers z, and the coefficient alpha used by the algorithm, and returns the set of centers S computed as specified by the algorithm. It is understood that the i-th integer in W is the weight of the i-th point in P. **Java users**: represent P and S as _ArrayList<Vector>_, and W as _ArrayList<Long>_. **Python users**: represent P and S as _list of tuple_ and W as _list of integers_. Considering the algorithm's high complexity, try to make the implementation as efficient as possible.

2. **Develop a method ComputeObjective(P,S,z)** which computes the value of the objective function for the set of points P, the set of centers S, and z outliers (the number of centers, which is the size of S, is not needed as a parameter). Hint: you may compute all distances d(x,S), for every x in P, sort them, exclude the z largest distances, and return the largest among the remaining ones. **Note that in this case we are not using weights**!

3. **Write a program GxxxHW2.java** (for Java users) **or GxxxHW2.py** (for Python users), where xxx is your 3-digit group number (e.g., 004 or 045), which receives in input the following command-line (CLI) arguments
   - **A path to a text file containing point set in Euclidean space**. Each line of the file contains, separated by commas, the coordinates of a point. Your program should make no assumptions on the number of dimensions!
     
   - **An integer k** (the number of centers).
     
   - **An integer z** (the number of allowed outliers). 
     
**The program must do the following:**
- Read the points in the input file into an ArrayList<Vector> (list of tuple in Python) called **inputPoints**. To this purpose, you can use the code provided in the file [InputCode.java](), for Java users, and [InputCode.py](), for Python users. 
  
- Create an ArrayList<Long> (list of integer in Python) called **weights** of the same cardinality of inputPoints, initialized with all 1's. (In this homework we will use unit weights, but in Homework 3 we will need the generality of arbitrary integer weights!). 
  
- Run **SeqWeightedOutliers(inputPoints,weights,k,z,0)** to compute a set of (at most) k centers. The output of the method must be saved into an ArrayList<Vector> (list of tuple in Python) called **solution**. 
  
- Run **ComputeObjective(inputPoints,solution,z)** and save the output in a variable called **objective**. 
  
- Return as output the following quantities: n =|P|, k, z, the initial value of the guess r, the final value of the guess r, and the number of guesses made by SeqWeightedOutliers(inputPoints,weights,k,z,0), the value of the objective function (variable objective), and the time (in milliseconds) required by the execution of SeqWeightedOutliers(inputPoints,weights,k,z,0). IT IS IMPORTANT THAT ALL PROGRAMS USE THE SAME OUTPUT FORMAT AS IN THE FOLLOWING EXAMPLE: [OutputFormat.txt]() 
  
Test your program using the datasets [available here]() (the outputs of some runs of the program on those datasets will be made available at the same link)
<br />

## Homework 3
In this homework, you will run a Spark program on the CloudVeneto cluster. The core of the Spark program will be the implementation of **2-round coreset-based MapReduce algorithm for k-center with z outliers**, which works as follows: in **Round 1**, separately for each partition of the dataset, a _weighted coreset_ of k+z+1 points is computed, where the weight assigned to a coreset point p is the number of points in the partition which are closest to p (ties broken arbitrarily); in **Round 2**, the L weighted coresets (one from each partition) are gathered into one weighted coreset of size (k+z+1)*L, and one reducer runs the sequential algorithm developed for Homework 2 (**SeqWeightedOutliers**) on this weighted coreset to extract the final solution. In the homework you will test the accuracy of the solution and the scalability of the various steps.

### Using CloudVeneto
A brief description of the cluster available for the course, together with instructions on how to access the cluster and how to run your program on it are given in this [User guide for the cluster on CloudVeneto]().

### Assignment
You must perform the following tasks.

**TASK 1. Download the template** ([TemplateHW3.java]() for Java users, and [TemplateHW3.py]() for Python users). The template contains a skeleton of the implementation of the 2-round MapReduce algorithm described above. Specifically, the template is structured as follows:
- Receives in input the following command-line (CLI) arguments:
  - **A path to a text file containing point set in Euclidean space**. Each line of the file contains, separated by commas, the coordinates of a point. Your program should make no assumptions on the number of dimensions!
    
  - **4 integers**: k (number of centers), z (number of outliers), and L (number of partitions). 
     
- Reads the input points into and RDD of Vector called **inputPoints**, subdivided into L partitions, sets the Spark configuration, and prints various statistics. 
   
- Runs a method **MR_kCenterOutliers** to compute the solution (i.e., a set of at most k centers) to the k-center problem with z outliers for the input dataset. The method implements the 2-round algorithm described. In Round 1 it extracts k+z+1 coreset points from each partition using method **kCenterFFT** which implements the Farthest-First Traversal algorithm, and compute the weights of the coreset points using method **computeWeights**. In Round 2, it collects the weighted coreset into a local data structure and runs method **SeqWeightedOutliers**, "recycled" from Homework 2, to extract and return the final set of centers (you must fill in this latter part). 
  
- Computes the value of the objective function for the returned solution (i.e., the maximum distance of a point from its closest center, excluding the z largest distances), using method **computeObjective**. 
  
- Prints the value of the objective function and the time taken by computeObjective. 
  
**TASK 2. Rename the template** into **GxxxHW3.java** (or **GxxxHW3.py**), where xxx is your 3-digit group number, and **complete the code** as follows (_you will see the parts where you must add code clearly marked in the template_):
1. Insert the code for SeqWeightedOuliers from your Homework 2. 
   
2. Complete Round 2 of MR_kCenterOutliers to extract and return the final solution. **IMPORTANT: you must run SeqWeightedOutliers on the weighted coreset using alpha=2**

3. Add suitable istructions to MR_kCenterOutliers, so to measure and print separately the time required by Round 1 and Round 2. Please be aware of the Spark's lazy evaluation.

4. Write the code for method computObjective. It is important that you keep in mind that the input dataset may be very large and that, in this case,  any structure of the size of this dataset may not fit into local memory.

The output of your code should use the following [OutputFormat](). (Note that the  lines "Initial guess", "Final Guess" and "Number of guesses" are those prinited by SeqWeightedOutliers, as in Homework 2).

**TASK 3. Test and debug your program** in local mode on your PC _to make sure that it runs correctly_. For this local test you can use the 16-point dataset testdataHW3.txt which you can [download here]() and the datasets uber-small.csv and artificial9000.txt that you used also for Homework 2, available in [this page]().

**TASK 4. Test your program on the cluster** using the datasets which have been preloaded in the HDFS available in the cluster. Use various configurations of parameters and report your results using the tables given in this word file: [TableHW3-Java.docx]() (for Java users) and [TableHW3-Python.docx]() (for Python users).

WHEN USING THE CLUSTER, YOU MUST STRICTLY FOLLOW THESE RULES:

- **To avoid congestion, groups with even (resp., odd) group number must use the clusters in even (resp., odd) days.**

- **Do not run several instances of your program at once.**

- **Do not use more than 16 executors.**

- **Try your program on a smaller dataset first.**

- **Remember that if your program is stuck for more than 1 hour, its execution will be automatically stopped by the system.**