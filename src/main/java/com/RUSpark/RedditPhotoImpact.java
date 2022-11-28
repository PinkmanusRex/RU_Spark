package com.RUSpark;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

import scala.Tuple2;

/* any necessary Java packages here */

public class RedditPhotoImpact {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditPhotoImpact <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		
		SparkSession spark = SparkSession
				.builder()
				.appName("RedditPhotoImpact")
				.getOrCreate();
		
//		Dataset<Row> ds = spark.read().option("inferSchema", true).csv(InputPath).repartition(20);

		List<Row> res = spark.read().option("inferSchema", true).csv(InputPath)
				.select(col("_c0").as("postId"), col("_c4").plus(col("_c5")).plus(col("_c6")).as("impactScore"))
				.groupBy("postId")
				.agg(sum("impactScore").as("impactScore"))
				.collectAsList();
		res.sort((a, b) -> {
			int postIdA = Integer.parseInt(a.getAs("postId").toString());
			int postIdB = Integer.parseInt(b.getAs("postId").toString());
			return postIdA - postIdB;
		});
		System.out.println(
					res.stream()
						.map(r -> String.format("%s %s", r.getAs("postId").toString(), r.getAs("impactScore").toString()))
						.collect(Collectors.joining("\n"))
				);

//		List<Tuple2<Integer, Integer>> res = spark.read().option("inferSchema", true).csv(InputPath)
//				.map((MapFunction<Row, Tuple2<Integer, Integer>>) r -> {
//					int id = r.getInt(0);
//					int impactScore = r.getInt(4) + r.getInt(5) + r.getInt(6);
//					return Tuple2.apply(id, impactScore);
//				}, Encoders.tuple(Encoders.INT(), Encoders.INT()))
//				.groupByKey((MapFunction<Tuple2<Integer, Integer>, Integer>) t -> t._1(), Encoders.INT())
//				.mapValues((MapFunction<Tuple2<Integer, Integer>, Integer>) t -> t._2(), Encoders.INT())
//				.reduceGroups((ReduceFunction<Integer>) (a, b) -> a + b)
//				.collectAsList();
//		
//		res.sort((a, b) -> a._1() - b._1());
//		System.out.println(
//					res
//						.stream()
//						.map(e -> e._1() + " " + e._2())
//						.collect(Collectors.joining("\n"))
//				);
	}
	
}
