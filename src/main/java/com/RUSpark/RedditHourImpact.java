package com.RUSpark;

import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.sum;

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
				.orderBy(col("hourOffset").asc())
				.collectAsList();

		System.out.println(
					res.stream()
						.map(r -> String.format("%s %s", r.getAs("hourOffset").toString(), r.getAs("impactScore").toString()))
						.collect(Collectors.joining("\n"))
				);
	}		
}
