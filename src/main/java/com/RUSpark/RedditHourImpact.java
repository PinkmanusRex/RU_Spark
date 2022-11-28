package com.RUSpark;

import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/* any necessary Java packages here */

public class RedditHourImpact {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditHourImpact <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		SparkSession spark = SparkSession
				.builder()
				.appName("RedditHourImpact")
				.getOrCreate();
		
		Dataset<Row> ds = spark.read().option("inferSchema", true).csv(InputPath);
		
		List<Tuple2<Integer, Integer>> res = ds
				.map((MapFunction<Row, Tuple2<Integer, Integer>>) r -> {
					long unixTime = r.getInt(1);
					int hourOffset = Instant.ofEpochSecond(unixTime).atZone(ZoneId.of("America/New_York")).toLocalTime().getHour();
					int impactScore = r.getInt(4) + r.getInt(5) + r.getInt(6);
					return Tuple2.apply(hourOffset, impactScore);
				}, Encoders.tuple(Encoders.INT(), Encoders.INT()))
				.groupByKey((MapFunction<Tuple2<Integer, Integer>, Integer>) t -> t._1(), Encoders.INT())
				.mapValues((MapFunction<Tuple2<Integer, Integer>, Integer>) t -> t._2(), Encoders.INT())
				.reduceGroups((ReduceFunction<Integer>) (a, b) -> a + b)
				.collectAsList();
		res.sort((a, b) -> a._1() - b._1());
		System.out.println(
					res
						.stream()
						.map(e -> e._1() + " " + e._2())
						.collect(Collectors.joining("\n"))
				);
	}

}
