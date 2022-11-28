package com.RUSpark;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

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
		
		Dataset<Row> ds = spark.read().option("inferSchema", true).csv(InputPath);
	
		List<Tuple2<Tuple2<Integer, Integer>, Integer>> res = ds
				.groupByKey((MapFunction<Row, Tuple2<Integer, Integer>>) r -> {
					int movieId = r.getInt(0);
					int rating = r.getInt(2);
					return Tuple2.apply(movieId, rating);
				}, Encoders.tuple(Encoders.INT(), Encoders.INT()))
				.mapValues((MapFunction<Row, Integer>) r -> r.getInt(1), Encoders.INT())
				.flatMapGroups((FlatMapGroupsFunction<Tuple2<Integer, Integer>, Integer, Tuple2<Integer, Integer>>) (k, vs) -> {
					List<Integer> customers = new ArrayList<>();
					while (vs.hasNext())
						customers.add(vs.next());
					List<Tuple2<Integer, Integer>> edges = new ArrayList<>();
					for (int i = 0; i < customers.size() - 1; i += 1) {
						int a = customers.get(i);
						for (int j = i + 1; j < customers.size(); j += 1) {
							int b = customers.get(j);
							if (a < b) {
								edges.add(Tuple2.apply(a, b));
							} else {
								edges.add(Tuple2.apply(b, a));
							}
						}
					}
					return edges.iterator();
				}, Encoders.tuple(Encoders.INT(), Encoders.INT()))
				.groupByKey((MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>) t -> Tuple2.apply(t._1(), t._2()), Encoders.tuple(Encoders.INT(), Encoders.INT()))
				.mapValues((MapFunction<Tuple2<Integer, Integer>, Integer>) t -> 1, Encoders.INT())
				.reduceGroups((ReduceFunction<Integer>)(a, b) -> a + b)
				.collectAsList();

		System.out.println(
					res
						.stream()
						.map(e -> String.format("(%d,%d) %d", e._1()._1(), e._1()._2(), e._2()))
						.collect(Collectors.joining("\n"))
				);
	}
}
