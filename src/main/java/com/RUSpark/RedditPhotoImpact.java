package com.RUSpark;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

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

		List<Row> res = spark.read().option("inferSchema", true).csv(InputPath)
				.select(col("_c0").as("postId"), col("_c4").plus(col("_c5")).plus(col("_c6")).as("impactScore"))
				.groupBy("postId")
				.agg(sum("impactScore").as("impactScore"))
				.orderBy(col("impactScore").desc(), col("postId"))
				.collectAsList();

		System.out.println(
					res.stream()
						.map(r -> String.format("%s %s", r.getAs("postId").toString(), r.getAs("impactScore").toString()))
						.collect(Collectors.joining("\n"))
				);

	}
	
}
