package com.example.demo;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import scala.Tuple2;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(DemoApplication.class, args);
		consumeKafkaMessages();
	}

	private static void consumeKafkaMessages() throws InterruptedException {
		System.setProperty("hadoop.home.dir", "D://hadoop");
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("WordCountingApp");
		//sparkConf.set("spark.cassandra.connection.host", "127.0.0.1");
		sparkConf.setMaster("local[*]");
		//sparkConf.set

		
		  JavaStreamingContext streamingContext = new JavaStreamingContext( sparkConf,
		  Durations.seconds(1));
		 
		
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream2");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		Collection<String> topics = Arrays.asList("messages");

		JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(streamingContext,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
		messages.print();
		
		JavaDStream<String> data = messages.map(v -> {
			return v.value(); // mapping to convert into spark D-Stream
		});
		data.print();
		 
		JavaPairDStream<String, String> results = messages.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
		results.print();
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
				
				streamingContext.start();
				streamingContext.awaitTermination();
	}
}
