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
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.sum;

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
		
		
//		Dataset<Row> ds = spark.read().option("inferSchema", true).csv(InputPath).repartition(20);
		
		spark
			.sqlContext()
			.udf()
			.register("convertToHourOffset", (Integer unixTime) -> {
				return Instant.ofEpochSecond(unixTime).atZone(ZoneId.of("America/New_York")).toLocalTime().getHour();
			}, DataTypes.IntegerType);

		List<Row> res = spark.read().option("inferSchema", true).csv(InputPath)
				.withColumn("hourOffset", callUDF("convertToHourOffset", col("_c1")))
				.select(col("hourOffset"), col("_c4").plus(col("_c5")).plus(col("_c6")).as("impactScore"))
				.groupBy("hourOffset")
				.agg(sum("impactScore").as("impactScore"))
				.collectAsList();
		res.sort((a, b) -> {
			int hourA = Integer.parseInt(a.getAs("hourOffset").toString());
			int hourB = Integer.parseInt(b.getAs("hourOffset").toString());
			return hourA - hourB;
		});
		System.out.println(
					res.stream()
						.map(r -> String.format("%s %s", r.getAs("hourOffset").toString(), r.getAs("impactScore").toString()))
						.collect(Collectors.joining("\n"))
				);
		
//		List<Tuple2<Integer, Integer>> res = spark.read().option("inferSchema", true).csv(InputPath)
//				.map((MapFunction<Row, Tuple2<Integer, Integer>>) r -> {
//					long unixTime = r.getInt(1);
//					int hourOffset = Instant.ofEpochSecond(unixTime).atZone(ZoneId.of("America/New_York")).toLocalTime().getHour();
//					int impactScore = r.getInt(4) + r.getInt(5) + r.getInt(6);
//					return Tuple2.apply(hourOffset, impactScore);
//				}, Encoders.tuple(Encoders.INT(), Encoders.INT()))
//				.groupByKey((MapFunction<Tuple2<Integer, Integer>, Integer>) t -> t._1(), Encoders.INT())
//				.mapValues((MapFunction<Tuple2<Integer, Integer>, Integer>) t -> t._2(), Encoders.INT())
//				.reduceGroups((ReduceFunction<Integer>) (a, b) -> a + b)
//				.collectAsList();
//		res.sort((a, b) -> a._1() - b._1());
//		System.out.println(
//					res
//						.stream()
//						.map(e -> e._1() + " " + e._2())
//						.collect(Collectors.joining("\n"))
//				);
	}

}
