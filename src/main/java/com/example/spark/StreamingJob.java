package com.example.spark;

import scala.Tuple2;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import org.apache.spark.streaming.Duration;
import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.*;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import java.util.Map;
import java.util.HashMap;

/*
1. Get a running kafka and zookeeper instance (conflent platform)

2. get prebuilt spark tar.gz and unzip

3. create a topic in kafka
bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test 

4. cd in spark repo and submit a job 
bin/spark-submit --class com.example.spark.StreamingJob --master local /home/qquartey/Desktop/maven.spark.kafka/target/streaming-job-1.0.0-SNAPSHOT.jar <zookeeper ip> <consumer-group-id> <topic>

5. cd in confluent and generate data
bin/ksql-datagen quickstart=users format=json topic=test maxInterval=1000
*/
public class StreamingJob {
    	
	static long sum = 0;
	public static void main(String args[])
	{
	// ensures that 3 arguments are entered
        if(args.length != 3)
        {
            System.out.println("SparkStream <zookeeper_ip> <group_nm> <topic1,topic2,...>");
            System.exit(1);
        }
	// suppresses info and warn to reduce clutter
	// allows for multiple topics
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        Map<String,Integer> topicMap = new HashMap<String,Integer>();
        String[] topic = args[2].split(",");
        for(String t: topic)
        {
            topicMap.put(t, new Integer(3));
        }
	
	// local streamingContext with 4 cores (spark needs 2 or more)and 1000ms batch interval
        JavaStreamingContext jssc = new JavaStreamingContext("local[4]", "SparkStream", new Duration(1000));
        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, args[0], args[1], topicMap );
	
        System.out.println("Connection done++++++++++++++");
        JavaDStream<String> data = messages.map(new Function<Tuple2<String, String>, String>() 
                                                {
						    @Override
                                                    public String call(Tuple2<String, String> message)
                                                    {
                                                        return message._2();
                                                    }
                                                }
                                                );
        System.out.println("Stream starts now");
	
	// print out actual messages, and time
	data.print();
	
	
	// display number of messages after 1 sec and stores the count in sum
	data.foreachRDD(new VoidFunction<JavaRDD<String>>() {
		 
            @Override
            public void call(JavaRDD<String> arg0) throws Exception {

                System.out.println(arg0.count()); 
		
		sum += arg0.count();
		System.out.println("total message count: " + sum);

            }
        }

        );

       
	try {
  	  jssc.start();
  	  jssc.awaitTermination();
	}catch (Exception ex ) {
  	  jssc.ssc().sc().cancelAllJobs();
  	  jssc.stop(true, false);
  	  System.exit(-1);
	}

    }
}
