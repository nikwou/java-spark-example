# Apache Spark data processing example in Java

## Intro

It took me some time to understand all the details for a working and sufficiently performant Apache Spark data processing example in Java. So, others might benefit from what I've learned on the way.

Imagine the following **use case**: you have a large and evolving CSV dataset containing 10,000+ financial data time series, e.g. stock prices. The size of the CSV is several hundred megabytes and the pattern for each entry (ie, a daily record for each time series) is:

    <TICKER>, <PERIOD>, <DATE>, <TIME>, <OPEN>, <HIGH>, <LOW>, <CLOSE>, <VOLUME>, <OPEN INTEREST>

A new entry is added daily for each time series; furthermore, new time series are occasionally added (eg due to new listings and IPOs). While the data is evolving, the dataset is provided as a static file, not as a stream.

For each time series in the dataset, we want to compute a number of KPIs like periodic highs/lows, moving averages and linear regressions on a daily basis. Given the amount of raw data, the job can hardly be done in real-time unless a massive computing cluster would be deployed. The requirement here is somewhat more relaxed: we want to get results in approx. 1 hour on a sufficiently powerful virtual cloud server instance. In other words: a real-world-application for Apache Spark without setting up a huge computing cluster.

The skeleton discussed here was originally developed using Apache Zeppelin and the Scala programming language, but once UDFs came into play, it was converted to Java. UDFs are very powerful instruments to add complex calculation functions to Apache Spark.

## Infrastructure concept

As mentioned above, we want to use cloud processing capabilities in order to get the processing done in a reasonable amount of time. Accordingly, the basic concept is as follows:

* Create and set up a Linux cloud server instance with 16 virtual cores, 32 GB of RAM and SSD storage
* Upload the Apache Spark application as a Maven package
* Build the jar 
* Run the jar (ie, the Apache Spark application) including the following steps:
    * Import CSV dataset
    * Process data
    * Persist results in MySQL database

The resulting MySQL database table - the "work product" - is then dumped and downloaded from the cloud server instance for further use and analysis. Creation and control of the cloud instance playbook is done by a Bash script. Using Docker is an alternative option, but currently, scripting appears to be easier and more flexible.

## The Java code

### Creation of Spark instance

It took me some time to understand the basic concept of an Apache Spark application. Basically, the code described below is a Java application that creates an Apache Spark instance and contains a set of data processing instructions. Such Apache Spark instance consists of at least two other JVMs, the Driver and the Executors. The first step is to create the Apache Spark instance:

    SparkSession spark = SparkSession
                .builder()
                .appName("SparkExample")
                .config("spark.master", "local[*]")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.driver.extraJavaOptions", "-XX:ReservedCodeCacheSize=512m -XX:+UseCompressedOops -XX:+UseG1GC -Xss1G")
                .config("spark.executor.extraJavaOptions", "-XX:ReservedCodeCacheSize=512m -XX:+UseCompressedOops -XX:+UseG1GC -Xss1G")
                //.config("spark.driver.memory", "8G")
                //.config("spark.executor.memory", "24G")
                //.config("spark.ui.port", "4040")
                .getOrCreate();

### Input CSV data


