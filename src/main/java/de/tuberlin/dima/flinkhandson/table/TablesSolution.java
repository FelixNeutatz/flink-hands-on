/**
 * Flink Hands-on
 * Copyright (C) 2015  Felix Neutatz
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.tuberlin.dima.flinkhandson.table;

import de.tuberlin.dima.flinkhandson.Config;
import de.tuberlin.dima.flinkhandson.table.utils.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.io.Serializable;

public class TablesSolution {
	
	public static class MyResult implements Serializable {
		public String title;
		public Double averageRating;
		public Integer userRatingCount;
	}

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		DataSet<Tuple3<Integer, Integer, Double>> rating =
				env.readCsvFile(Config.pathTo("rating.csv"))
						.fieldDelimiter(",")
						.ignoreFirstLine() //ignore schema
						.types(Integer.class, Integer.class, Double.class);

		DataSet<Tuple3<Integer, String, String>> movie =
				env.readCsvFile(Config.pathTo("movie.csv"))
						.fieldDelimiter(",")
						.ignoreFirstLine() //ignore schema
						.types(Integer.class, String.class, String.class);

		// IMPLEMENT THIS STEP
		Table movieTable = tableEnv.fromDataSet(movie, "movieId, title, genre");
		Table ratingTable = tableEnv.fromDataSet(rating, "userId, movieId, rating");
		
		Table joined = ratingTable.join(movieTable.select("movieId as m, title, genre")).where("movieId = m");
		
		Table movieAVG = joined.groupBy("movieId, title")
				.select("title, rating.avg as averageRating, userId.count.cast(INT) as userRatingCount")
				.filter("userRatingCount > 2");
				
		DataSet<MyResult> result = tableEnv.toDataSet(movieAVG, MyResult.class); //get result in form of a POJO
		DataSet<Tuple2<String,Double>> resultTuples = result.map(new MyResult2Tuple()); //convert POJO to Tuple2
		
		Utils.plot(resultTuples, true, "Average movie rating", "Movies / TV shows", "Average Rating"); //plot a fancy chart :)
		
	}

	public static class MyResult2Tuple implements MapFunction<MyResult, Tuple2<String,Double>> {
		@Override
		public Tuple2<String,Double> map(MyResult in) {
			return new Tuple2<String,Double>(in.title, in.averageRating);
		}
	}
}
