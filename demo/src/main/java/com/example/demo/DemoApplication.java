package com.example.demo;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) throws InterruptedException, TimeoutException, StreamingQueryException {
		SpringApplication.run(DemoApplication.class, args);
		consumeKafkaMessages();
	}

	private static void consumeKafkaMessages() throws InterruptedException, TimeoutException, StreamingQueryException {
		System.setProperty("hadoop.home.dir", "D://hadoop");
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("WordCountingApp");
		//sparkConf.set("spark.cassandra.connection.host", "127.0.0.1");
		sparkConf.setMaster("local[*]");
		//sparkConf.set

		SparkSession session = SparkSession
				  .builder()
				  .appName("Java Spark SQL basic example")
				  .master("local[*]")
				  .getOrCreate();
		//session.sparkContext().setLogLevel("DEBUG");
		
		/*
		 * JavaStreamingContext streamingContext = new JavaStreamingContext( sparkConf,
		 * Durations.seconds(1));
		 */
		 
		
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		//kafkaParams.put("key.deserializer", StringDeserializer.class);
		//kafkaParams.put("value.deserializer", StringDeserializer.class);
		//kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream2");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", "false");
		Collection<String> topics = Arrays.asList("messages");

		session.udf().register("sessionDurationFn", new UDF1<String, Long>() {
			@Override
			public Long call(String messageValue) throws Exception {
			String[]  strArr = messageValue.split(",");
			return Long.parseLong(strArr[0]);
			}
			}, DataTypes.LongType);
		
			session.udf().register("userNameFn", new UDF1<String, String>() {
			@Override
			public String call(String messageValue) throws Exception {
			String[]  strArr = messageValue.split(",");
			return strArr[1];
			}
			}, DataTypes.StringType);
			
		Dataset<Row> df = session
				  .readStream()
				  .format("kafka")
				 // .options(kafkaParams)
				  .option("auto.offset.reset", "latest")
				  .option("enable.auto.commit", false)
				  .option("group.id", "use_a_separate_group_id_for_each_stream3")
				  .option("kafka.bootstrap.servers", "localhost:9092")
				  .option("subscribe", "messages")
				  .load();
				df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
				
		df.printSchema();
		df.createOrReplaceTempView("session_data_init");
		Dataset<Row> preresults = session.sql("select sessionDurationFn(cast (value as string)) as "
				+ "session_duration, userNameFn(cast (value as string)) as userName,timestamp from session_data_init");
		preresults.createOrReplaceTempView("session_data");
		Dataset<Row> results = session.sql("select window,sum(session_duration) as session_duration,userName "
				+ " from session_data group by window(timestamp,'2 minutes'),userName");
		        StreamingQuery query = results.writeStream()
		.format("console")
		.outputMode(OutputMode.Update())
		.option("truncate", false)
		.option("numRows", 50)
		.start();
		query.awaitTermination();  
		//System.out.println(df);
		//df.show();
		
		  //df.writeStream().outputMode("append") .format("console") .start();
		  //df.show();
		 

			/*
			 * StreamingQuery ds = df .selectExpr("CAST(key AS STRING)",
			 * "CAST(value AS STRING)") .writeStream() .format("kafka")
			 * .option("checkpointLocation", "D://girish")
			 * .option("kafka.bootstrap.servers", "localhost:9092") .option("topic",
			 * "output") .start();
			 */
				
		/*
		 * JavaInputDStream<ConsumerRecord<String, String>> messages =
		 * KafkaUtils.createDirectStream(streamingContext,
		 * LocationStrategies.PreferConsistent(), ConsumerStrategies.<String,
		 * String>Subscribe(topics, kafkaParams)); messages.print();
		 * 
		 * JavaDStream<String> data = messages.map(v -> { return v.value(); // mapping
		 * to convert into spark D-Stream }); data.print();
		 */
		 
		/*
		 * JavaPairDStream<String, String> results = messages.mapToPair(record -> new
		 * Tuple2<>(record.key(), record.value())); results.print();
		 */
		/*
		 * JavaDStream<String> lines = results .map( tuple2 -> tuple2._2() );
		 * JavaDStream<String> words = lines .flatMap( x ->
		 * Arrays.asList(x.split("\\s+")).iterator() ); JavaPairDStream<String, Integer>
		 * wordCounts = words .mapToPair( s -> new Tuple2<>(s, 1) ).reduceByKey( (i1,
		 * i2) -> i1 + i2 ); wordCounts.foreachRDD( javaRdd -> { Map<String, Integer>
		 * wordCountMap = javaRdd.collectAsMap(); for (String key :
		 * wordCountMap.keySet()) { List<Integer> wordList =
		 * Arrays.asList(wordCountMap.get(key)); JavaRDD<Integer> rdd =
		 * streamingContext.sparkContext().parallelize(wordList);
		 * System.out.println("hello test: " + wordCountMap.get(key)); List<Integer>
		 * list = rdd.collect(); System.out.println("hello test:" + list);
		 * 
		 * } } );
		 */
		
		/*
		 * OffsetRange[] offsetRanges = { // topic, partition, inclusive starting
		 * offset, exclusive ending offset OffsetRange.create("messages", 0, 0, 100),
		 * OffsetRange.create("messages", 1, 0, 100) };
		 * 
		 * RDD<ConsumerRecord<Object, Object>> rdd = KafkaUtils.createRDD( new
		 * SparkContext(sparkConf), kafkaParams, offsetRanges,
		 * LocationStrategies.PreferConsistent() ); rdd.count();
		 * 
		 * System.out.println(rdd.count());
		 */
				
		//JavaPairDStream<String, String> results = messages.
				//.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
		 
		/*
		 * JavaDStream<String> lines = results .map( tuple2 -> tuple2._2() );
		 * JavaDStream<String> words = lines .flatMap( x ->
		 * Arrays.asList(x.split("\\s+")).iterator() ); JavaPairDStream<String, Integer>
		 * wordCounts = words .mapToPair( s -> new Tuple2<>(s, 1) ).reduceByKey( (i1,
		 * i2) -> i1 + i2 );
		 */
				
		/*
		 * results.foreachRDD( javaRdd -> { Map<String, String> wordCountMap =
		 * javaRdd.collectAsMap(); for (String key : wordCountMap.keySet()) {
		 * System.out.println(wordCountMap.get(key)); } } );
		 */
				
		/*
		 * streamingContext.start(); streamingContext.awaitTermination();
		 */
	}
}
