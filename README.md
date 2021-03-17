# Apache Spark data processing example in Java

## Intro

It took me some time to understand all the details for a working and sufficiently performant Apache Spark data processing example in Java. So, others might benefit from what I've learned on the way.

Imagine the following ***use case***: you have a large and evolving CSV dataset containing 10,000+ financial data time series, e.g. stock prices. The size of the CSV is several hundred megabytes and the pattern for each entry (ie, a daily record for each time series) is:

    <TICKER>, <PERIOD>, <DATE>, <TIME>, <OPEN>, <HIGH>, <LOW>, <CLOSE>, <VOLUME>, <OPEN INTEREST>

New entries are added each day for each time series; furthermore, new time series are added. While the data is evolving, however, the dataset is provided as a static file, not as a stream.

For each time series in the dataset, we want to compute a number of KPIs like periodic highs/lows, moving averages and linear regressions. Some KPIs are only needed for the most up-to-date Given the amount of raw data, the job can hardly be done in real-time unless a massive computing cluster would be deployed. The requirement here is somewhat more relaxed: we want to get results in approx. 1 hour and use a virtual cloud server (based on Index Xeon Gold CPUs) with 16 virtual cores, 32 GB of RAM and SSD storage. In other words: a real-world-application for Apache Spark without setting up a huge computing cluster.

The skeleton discussed here was originally developped using Apache Zeppelin and the Scala programming language, but once UDFs came into play, it was converted to Java. UDFs are very powerful instruments to add complex calculation functions to Apache Spark.
