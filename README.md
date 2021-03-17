# Apache Spark data processing example in Java

## Intro

It took me some time to understand all the details for a working and sufficiently performant Apache Spark data processing example in Java. So, others might benefit from what I've learned on the way.

Imagine the following **use case**: you have a large and evolving CSV dataset containing 10,000+ financial data time series, e.g. stock prices. The size of the CSV is several hundred megabytes and the pattern for each entry (ie, a daily record for each time series) is:

    <TICKER>, <PERIOD>, <DATE>, <TIME>, <OPEN>, <HIGH>, <LOW>, <CLOSE>, <VOLUME>, <OPEN INTEREST>

A new entry is added daily for each time series; furthermore, new time series are occasionally added (eg due to new listings and IPOs). While the data is evolving, the dataset is provided as a static file, not as a stream.

For each time series in the dataset, we want to compute a number of KPIs like periodic highs/lows, moving averages and linear regressions on a daily basis. Given the amount of raw data, the job can hardly be done in real-time unless a massive computing cluster would be deployed. The requirement here is somewhat more relaxed: we want to get results in approx. 1 hour on a sufficiently powerful virtual cloud server instance. In other words: a real-world-application for Apache Spark without setting up a huge computing cluster.

The skeleton discussed here was originally developed using Apache Zeppelin and the Scala programming language, but once UDFs came into play, it was converted to Java. UDFs are very powerful instruments to add complex calculation functions to Apache Spark.

Conceptually, it may be worth noting that the result of the exercise is a table equal to the input table/CSV, but with more columns; each column holds a specific data analysis result, eg a moving average computation or the result of a linear regression calculation.

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

It took me some time to understand the basic concept of an Apache Spark application. Basically, the code described below is a Java application that creates an Apache Spark instance and contains a set of data processing instructions; finally, the result is persisted in a MySQL database. The Apache Spark instance itself consists of at least two other JVMs, the Driver and the Executors. 

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

Before importing CSV data, the basic structure must be presented to the SparkSession. Like a database table definition, the SparkSession needs to know what columns are available including the type of each column. You will notice the similarity of the structure below and the CSV file format detailed at the above:

    ArrayList<StructField> fields = new ArrayList<>();
    
        fields.add(DataTypes.createStructField("Ticker", org.apache.spark.sql.types.DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("Per", org.apache.spark.sql.types.DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("rawDate", org.apache.spark.sql.types.DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("Time", org.apache.spark.sql.types.DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("Open", org.apache.spark.sql.types.DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("High", org.apache.spark.sql.types.DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("Low", org.apache.spark.sql.types.DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("Close", org.apache.spark.sql.types.DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("Volume", org.apache.spark.sql.types.DataTypes.LongType, false));
        fields.add(DataTypes.createStructField("OpenInt", org.apache.spark.sql.types.DataTypes.LongType, false));
    
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




            



