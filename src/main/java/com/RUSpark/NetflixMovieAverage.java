package com.RUSpark;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/* any necessary Java packages here */

public class NetflixMovieAverage {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixMovieAverage <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		SparkSession spark = SparkSession
				.builder()
				.appName("NetflixMovieAverage")
				.getOrCreate();
		
		spark.read().option("inferSchema", true).csv(InputPath).createOrReplaceGlobalTempView("NetflixTempTable");
		
		spark.sql("SELECT _c0 AS movieId, avg(_c2) AS avg FROM global_temp.NetflixTempTable GROUP BY _c0").show();
		
//		List<Tuple2<Integer, Double>> res = spark.read().option("inferSchema", true).csv(InputPath)
//				.groupByKey((MapFunction<Row, Integer>) r -> r.getInt(0), Encoders.INT())
//				.mapValues((MapFunction<Row, Integer>) r -> r.getInt(2), Encoders.INT())
//				.mapGroups((MapGroupsFunction<Integer, Integer, Tuple2<Integer, Double>>) (k, vs) -> {
//					double sum = 0.0;
//					int noEntries = 0;
//					while (vs.hasNext()) {
//						double v = vs.next();
//						sum += v;
//						noEntries += 1;
//					}
//					return Tuple2.apply(k, sum/noEntries);
//				}, Encoders.tuple(Encoders.INT(), Encoders.DOUBLE()))
//				.collectAsList();
//		
//		res.sort((a, b) -> a._1() - b._1());
//		System.out.println(
//					res
//						.stream()
//						.map(e -> String.format("%d %.2f", e._1(), e._2()))
//						.collect(Collectors.joining("\n"))
//				);
	}

}
