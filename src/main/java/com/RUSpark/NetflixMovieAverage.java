package com.RUSpark;

import java.text.NumberFormat;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.avg;

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
		
		List<Row> res = spark.read().option("inferSchema", true).csv(InputPath)
			.groupBy("_c0")
			.agg(avg("_c2").as("_c2"))
			.collectAsList();
		res.sort((a, b) -> {
			int movieIdA = a.getInt(0);
			int movieIdB = b.getInt(0);
			return movieIdA - movieIdB;
		});
		NumberFormat formatter = NumberFormat.getNumberInstance(Locale.US);
		formatter.setMinimumFractionDigits(0);
		formatter.setMaximumFractionDigits(2);
		System.out.println(
					res.stream()
						.map(r -> String.format("%d %s", r.getInt(0), formatter.format(r.getDouble(1))))
						.collect(Collectors.joining("\n"))
				);
		
	}

}
