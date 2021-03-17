# Apache Spark data processing example in Java

## Intro

It took me some time to understand all the details for a working and sufficiently performant Apache Spark data processing example in Java. So, others might benefit from what I've learned on the way.

Imagine the following use case: you have a large and evolving CSV dataset containing 10,000+ data time series, eg stock market prices (it could also be product sales or something else). The size of the CSV is several hundred megabytes and the pattern for each observation (ie, a daily record for each time series) is:

    <TICKER>, <DATE>, <OPEN>, <HIGH>, <LOW>, <CLOSE>, <VOLUME>

A new observation is added daily for each time series; furthermore, new time series are occasionally added (eg due to new listings and IPOs). While the data is evolving, the dataset is provided as a static file, not as a stream.

For each time series in the dataset, we want to compute a number of KPIs like percentage change and linear regressions on a daily basis. Given the amount of raw data, the job can hardly be done in real-time unless a massive computing cluster would be deployed. The requirement here is somewhat more relaxed: we want to get results on a sufficiently powerful cloud server instance. In other words: a real-world-application for Apache Spark, but without setting up a huge computing cluster.

The skeleton discussed here was originally developed using Apache Zeppelin and the Scala programming language, but once UDFs came into play, it was converted to Java. UDFs are very powerful instruments to add complex calculation functions to Apache Spark.

Conceptually, it may be worth noting that the result of the exercise is a table equal to the input table/CSV, but with more columns; each column holds a specific data analysis result, eg a percent change computation or the result of a linear regression calculation.

## Infrastructure concept

As mentioned above, we want to use cloud processing capabilities in order to get the processing done within a reasonable amount of time. Accordingly, the basic concept is as follows:

* Create and set up a cloud server instance with (eg 16 virtual cores, 32 GB of RAM and SSD storage; Linux operating system)
* Upload the Apache Spark application as a Maven package
* Build the jar 
* Run the jar (ie, the Apache Spark application) including the following steps:
    * Import CSV dataset
    * Process data
    * Persist results in MySQL database

The resulting MySQL database table - the "work product" - is then dumped and downloaded from the cloud server instance for further use and analysis. Creation and control of the cloud instance playbook is done by a Bash script. Using Docker is an alternative option, but currently, scripting appears to be easier and more flexible.

## The Java code

It took me some time to understand the basic concept of an Apache Spark application. Basically, the code described below is a Java application that creates an Apache Spark instance and contains a set of data processing instructions. Once the data processing has been done, the result is persisted in a MySQL database. During processing, apart from the JVM running the Java application, the Apache Spark instance will be running other JVMs, namely the Driver and the Executors. During execution, one may open a browser connection on port 4040 to see the Apache Spark GUI and get an update on processing progress.

### Creation of Spark instance

The first step is to create an Apache Spark instance:

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

Before importing CSV data, the basic structure must be presented to the SparkSession. Like a database table definition, the SparkSession needs to know what columns are available including the type of each column. You will notice the similarity of the structure below and the CSV file format detailed above:

    ArrayList<StructField> fields = new ArrayList<>();
    
        fields.add(DataTypes.createStructField("Ticker", org.apache.spark.sql.types.DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("rawDate", org.apache.spark.sql.types.DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("Open", org.apache.spark.sql.types.DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("High", org.apache.spark.sql.types.DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("Low", org.apache.spark.sql.types.DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("Close", org.apache.spark.sql.types.DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("Volume", org.apache.spark.sql.types.DataTypes.LongType, false));
        
        StructType schemata = DataTypes.createStructType(fields);
    
    Dataset<Row> df = spark.read()
            .format("csv")
            .option("header", "true")
            .option("delimiter", ",")
            .schema(schemata)
            .load("/tmp/data.csv");
            
## UDF definition

User-defined functions are coded procedures which can be called by Apache Spark to fulfill certain processing functionalities. The purpose of the UDF definition below is a linear regression calculation which hands back slope, intercept and r2 as results. Linear regression is provided by the Apache Spark framework, but the performance seemed not appropriate given the amount of data to be processed. The UDF uses the Java SimpleRegression funtionality. As will be shown below, the time series Dates are converted into unix timestamps as one of the first processing steps, the reason being that such conversion keeps the door open for later intra-day time series processing.

    spark.udf().register("movingAvg", new UDF3<Boolean, WrappedArray<Long>, WrappedArray<Double>, Row>() {

        private static final long serialVersionUID = -2270493948394875705L;
        @Override

        public Row call(Boolean analysisDate, WrappedArray<Long> dates, WrappedArray<Double> close) throws Exception {

            if (analysisDate ) {
            
                ArrayList<Long> datesJava = new ArrayList<Long>(JavaConverters
                        .asJavaCollectionConverter(dates)
                        .asJavaCollection());

                ArrayList<Double> closeJava = new ArrayList<Double>(JavaConverters
                        .asJavaCollectionConverter(close)
                        .asJavaCollection());
                
                Long oldestUnixTS = datesJava.get(0);
                
                SimpleRegression sReg = new SimpleRegression();

                int j = dates.length();
                
                for (int i=0; i<j; i++) 
                    
                    sReg.addData(datesJava.get(i)-oldestUnixTS,closeJava.get(i));
                
                return RowFactory.create(Math.round(sReg.getSlope()*100.0D)/100.0D,
                                            Math.round(sReg.getIntercept()*100.0D)/100.0D,
                                            Math.round(sReg.getRSquare()*100.0D)/100.0D);
                
            } else
                
                return RowFactory.create(0.0D, 0.0D, 0.0D);
            
        }
        
    }, DataTypes.createStructType(Arrays.asList(
            DataTypes.createStructField("slope", DataTypes.DoubleType, false), 
            DataTypes.createStructField("intercept", DataTypes.DoubleType, false),
            DataTypes.createStructField("r2", DataTypes.DoubleType, false))));

## Individual processing steps

Having finished UDF definition, we can turn to the main body of the data processing instructions code. As far as my understanding goes, ordering (repartitioning) data by time series (Ticker) is a useful step for performance optimisation: such repartition ensures each complete time series is processed by only one executor core, thereby avoiding to shuffle around data which is a resource-intensive process.

    df = df.repartition(df.col("Ticker"));

Then, as mentioned above, another column showing the Unix timestamp representation of each given date is added:
    
    df = df.withColumn("Date", functions.to_date(df.col("rawDate"), "yyyyMMdd"));
    df = df.drop("rawDate");
    
    df = df.withColumn("unixTS", (functions.unix_timestamp(df.col("Date")).divide(86400)).cast("long"));

Assuming that certain analysis functions should only be applied to the youngest time series observation, it seemed reasonable to introduce a "switch" indicating whether the respective analysis function (ie a computation-intensive linear regression UDF call) shall be done:
    
    df = df.withColumn("analysisDatePrep", functions.max("unixTS").over(fullRange) );
    df = df.withColumn("analysisDate", df.col("unixTS").equalTo(df.col("analysisDatePrep")) );
    df = df.drop("analysisDatePrep");
    
Again, as a basic and fundamental point of understanding, please note that Apache Spark likes ordering the very large "spreadsheet" to be processed in a format where each individual observation of a time series constitutes a line. Processing results are then added as additional columns for each observation. Adding columns is all-or-nothing, one cannot selectively add the columns for certain time series observations while avoiding others. Accordingly, if one tries to be efficient, the "analysisDate" switch signals to Apache Spark if a calculation needs to be  done for a specific observation.

What follows is an example of the processing exercise. Here, percent change figures for 3, 10 and 60 days are calculated. As mentioned, the respective calculation results are added to each data entry as an additional column. You may notice the processing involves two subsequent steps, first fetching the respective previous Close figure and then calculating the percent change figure.
    
    WindowSpec pctChg = Window.partitionBy("Ticker").orderBy("unixTS");

    df = df.withColumn("prevClose3", functions.lag(df.col("Close"), 3).over(pctChg) );
    df = df.withColumn("prevClose10", functions.lag(df.col("Close"), 10).over(pctChg) );
    df = df.withColumn("prevClose60", functions.lag(df.col("Close"), 60).over(pctChg) );
    
    df = df.withColumn("pctChg3", functions.when(df.col("analysisDate"), functions.round(df.col("Close").divide(df.col("prevClose3")).minus(1),2) ).otherwise(null));
    df = df.withColumn("pctChg10", functions.when(df.col("analysisDate"), functions.round(df.col("Close").divide(df.col("prevClose10")).minus(1),2) ).otherwise(null));
    df = df.withColumn("pctChg60", functions.when(df.col("analysisDate"), functions.round(df.col("Close").divide(df.col("prevClose60")).minus(1),2) ).otherwise(null));

Furthermore, it may be interesting to see how the UDF defined above can be called. An excerpt of the dataset - consisting of Unix timestamps and Closing prices for each observation - is handed over to the UDF function call. The result is again appended as a new column "linReg365". The resulting Struct (slope, intercept, r2) is then expanded into three columns. This is useful to avoid errors during the subsequent database persistence step.

	WindowSpec linReg365 = Window.partitionBy("Ticker").orderBy("unixTS").rangeBetween(-364, 0);

	df = df.withColumn("linReg365", functions.when(df.col("analysisDate"), functions.callUDF("movingAvg", df.col("analysisDate"), functions.collect_list(df.col("unixTS")).over(linReg365), functions.collect_list(df.col("Close")).over(linReg365) )).otherwise(null)); 

    df = df.withColumn("linReg365_slope", df.col("linReg365.slope"))
        .withColumn("linReg365_intercept", df.col("linReg365.intercept"))
        .withColumn("linReg365_r2", df.col("linReg365.r2"))
        .drop("linReg365");
		
    df = df.na().fill(0);

## Persisting results to MySQL database

As a last step, results are persisted into a MySQL database. Elevating the "batchsize" option appears useful for speedier processing. Also, younger generations of Apache Spark do not need the "driver" option as the required JDBC driver is automatically selected in accordance with the "url" definition; accordingly, this step is commented out below. 

    df.write()
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/<database name>?serverTimezone=Europe/Berlin")
        .option("batchsize", 100000)
        //.option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", "<table name>")
        .option("user", "<enter user name here>")
        .option("password", "<enter user pw here>")
        .mode(SaveMode.Overwrite)
        .save();
        
After storing data in the MySQL table, data access performance can be improved significantly by adding indexes to the database. The respective MySQL instruction would be:

    CREATE INDEX idxTicker ON table (Ticker(10));
    
For further data analysis, tools like Grafana or Apache Superset are quite useful.
