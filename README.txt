Project Authors: 
Michael Silber
Elior Tapiro


Output URL: https://assignment2-hadoop.s3.amazonaws.com/result/part-r-00000



----------------------
HOW TO RUN THE PROJECT
--------------------------------------------------------------------------------------------------------------------------------------------
1. To run the project you must firstly change the myKeyPair field in the Main.java file to be your key file
name, also change the myBucketName field to be the name of the bucket you intend to work on.
2. Place all the map-reduce jars into your bucket, and open an input folder where you will place all your input into.
The bucket should look like the following directory tree:
	-myBucket
		-input
			-hebrew-3grams
			-anyotherfiles
		-CrossValCount.jar
		-JoinAndCalc.jar
		-RCount.jar
		-Sort.jar
		-TriGrams.jar

3. Finally run the ass2 jar as follows in the terminal after creating a runnable JAR from Main.java:
	java -jar ass2.jar
--------------------------------------------------------------------------------------------------------------------------------------------

--------------------------------
Description of Map-Reduce steps:
--------------------------------------------------------------------------------------------------------------------------------------------
1) TriGrams
	This map-reduce job firstly filters any non-trigrams from the corpus, and then randomly places each trigram into one
	of two groups for the cross validation. Finally in the reducer it sums all trigrams of the same type in their groups.
	For example for "dani went home" (in hebrew) we may get 53 of those trigrams in Group 0, and 46 in Group 1.

2) RCount and CrossValidationCount
	Here the mapper submits a key-value pair twice for each trigram in order to count how many trigrams appeared R times
	in each 

3) JoinAndCalc
	All the job here relies on the fact that at this stage the key-value pairs from RCount and CrossValidationCount have the
	same key for each trigram, namely that the key is the trigram itself, thereby allowing an easy join of the two. Furthermore,
	the keys are the trigram so the reducers will receive all the data for each trigram in the same place allowing a join in the reducer.
	The reducer will receive 4 values for each trigram, the number of trigrams appearing R0 times in Group 0, the number of trigrams
	appearing R1 times in Group 1, the crossvalidation (TR01) number of times that trigrams appearing R0 times in Group 0
	appeared in Group 1, and the crossvalidation (TR10) number of times that trigrams appearing R1 times in Group 1 appeared
	in Group 0. This in addition to the number N which was written by the Trigrams m-r job gives us all the info needed in order
	to calculate the probability for the trigram using deleted estimation.

4) ValueToKeySort
	By this stage we have all the results, we simply wanted to sort them according to the first two words in the trigram primarily,
	and secondarily by the probability descending if the first two words are equal in two different trigrams. Because we want to be scalable
	and make no assumptions about memory, we decided to use a ValueToKey sort which allowed the partitioner to do all the sorting for us
	thereby not using a sort method which would be unscalable.

---------------------
Combiner Explanation:
--------------------------------------------------------------------------------------------------------------------------------------------------
1) Trigrams
	We used a combiner to locally aggregate the sums of the trigrams before sending to the reducer, the code is identical to the reducer.
	See the Statistics doc for the stats on the performance with and without the combiner.

2) RCount and CrossValidationCount
	We couldn't use a combiner for this map-reduce job because we rely on the values in the key-value pairs (the trigram is in the value) to
	later be used in the JoinAndCalc job for the join process, so therefore local aggregation before the reducer would make us lose this important
	data in the value.

3) JoinAndCalc
	Here we also couldn't use a combiner (also logically wouldn't make sense because there are specifically 4 values for each key) because the aggregation
	process calculates with division which isn't associative as we've seen in class.

4) ValueToKeySort
	The mapper simply moves the data forward to be sorted by the partitioner and therefore there is no work to be done by a combiner in this map-reduce job.




