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

* Create and set up a cloud server instance with 16 virtual cores, 32 GB of RAM and SSD storage
* Upload the Apache Spark application as a Maven package
* Build the jar 
* Run the jar (ie, the Apache Spark application) including the following steps:
    * Import CSV dataset
    * Process data
    * Persist results in MySQL database

The resulting MySQL database table - the "work product" - is then dumped and downloaded from the cloud server instance for further use and analysis.

