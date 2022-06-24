bin/spark-submit \
--class org.apache.spark.examples.SparkPi \
--master yarn \
--deploy-mode cluster \
examples/jars/spark-examples_2.11-2.4.3.jar