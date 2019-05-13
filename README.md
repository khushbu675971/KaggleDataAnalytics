Pre-Requisite:

Installation of SBT, Scala and Spark

How to run:

1. Create JAR using below

   sbt clean package
   
2. Run using spark-submit from terminal

  a. Go to the project location and hit below command by setting master(yarn, local) and JAR location.
  
  b. Pass the source files location as arguments for handling changes of files every day

spark-submit --class KaggleApp --master local[8] target/scala-2.12/kaggleanalytics_2.12-0.1.jar src/main/resources/googleplaystore.csv src/main/resources/googleplaystore_user_reviews.csv

spark-submit --class KaggleApp --master yarn --deploy-mode cluster target/scala-2.12/kaggleanalytics_2.12-0.1.jar src/main/resources/googleplaystore.csv src/main/resources/googleplaystore_user_reviews.csv

Note: For running in Yarn, yarn cluster should be available

Computation covered in this solution:

1. This application covers sql queries computation for exercise 1-6. For exercise 7 this application takes source files location from command file so that can run
daily basis based on provided input file path. For consume by a dashboard this application is able to provide the data in relational database(Please find the code commented out for each exercise).

2. This application clean the data for missing values, violating data type (Handled by applying the schema), remove the garbage rows,  remove double quotes for some data, and able to handle NaN values.
Note: Find the data cleaning steps while creating rowRDD.

3. For exercise 5, for optimizing join, filtering the data having positiveCount > (negCount + neutCount) and applied inner join based on key app.

4. For the data size would be in the range of terabytes instead of megabytes?
 This solution is developed in spark framework which is able to run parallel computation on huge datasets.

