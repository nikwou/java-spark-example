# Apache Spark data processing example in Java

## Introduction

It took me some time to work out all the details for a working - and sufficiently performant - Apache Spark data processing example in Java, so others might benefit from what I've learned.

Imagine the following use case: you have a large and evolving CSV dataset containing several thousand time series. The size of the CSV is several hundred megabytes. New entries are added each day for each time series; furthermore, new time series are added. While the data is evolving, the dataset is provided as a static file, not as a stream.

For each time series in the dataset, we want to compute a number of KPIs like periodic highs/lows, moving averages and linear regressions. Given the amount of raw data, the job can hardly be done in real-time unless a massive computing cluster would be deployed. The requirement here is somewhat more relaxed: we want to get results in approx. 1 hour and use a virtual cloud server (based on Index Xeon Gold CPUs) with 16 virtual cores, 32 GB of RAM and SSD storage. In other words: a real-world-application for Apache Spark without setting up a huge computing cluster.

The skeleton discussed here was originally developped using Apache Zeppelin and the Scala programming language, but once UDFs came into play, it was converted to Java. UDFs are very powerful instruments to add complex calculation functions to Apache Spark.

## Apache Spark performance tuning

## The skeleton
