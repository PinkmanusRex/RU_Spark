package com.RUSpark;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;

/* any necessary Java packages here */

public class NetflixGraphGenerate {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixGraphGenerate <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		SparkSession spark = SparkSession
				.builder()
				.appName("NetflixGraphGenerate")
				.getOrCreate();
		
		List<Row> res = spark.read().option("inferSchema", true).csv(InputPath)
				.select(col("_c0").as("movieId"), col("_c2").as("rating"), col("_c1").as("customerId"))
				.groupBy("movieId", "rating")
				.agg(collect_list("customerId").as("customerIds"))
				.flatMap((FlatMapFunction<Row, Row>) r -> {
					List<Integer> customerIds = r.getList(2);
					List<Row> pairs = new ArrayList<>();
					for (int i = 0; i < customerIds.size() - 1; i += 1) {
						Integer customerA = customerIds.get(i);
						for (int j = i + 1; j < customerIds.size(); j += 1) {
							Integer customerB = customerIds.get(j);
							if (customerA < customerB)
								pairs.add(RowFactory.create(customerA, customerB));
							else
								pairs.add(RowFactory.create(customerB, customerA));
						}
					}
					return pairs.iterator();
				}, RowEncoder.apply(new StructType().add("customerA", DataTypes.IntegerType).add("customerB", DataTypes.IntegerType)))
				.groupBy("customerA", "customerB")
				.count()
				.orderBy(col("count").desc(), col("customerA"), col("customerB"))
				.collectAsList();

		System.out.println(
					res.stream()
						.map(r -> String.format("(%d,%d) %d", r.getInt(0), r.getInt(1), r.getLong(2)))
						.collect(Collectors.joining("\n"))
				);
	
	}
}
