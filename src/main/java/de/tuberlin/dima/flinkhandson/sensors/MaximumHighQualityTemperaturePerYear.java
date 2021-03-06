/**
 * Flink Hands-on
 * Copyright (C) 2014  Sebastian Schelter
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

package de.tuberlin.dima.flinkhandson.sensors;

import de.tuberlin.dima.flinkhandson.Config;
import org.apache.commons.math.stat.descriptive.rank.Max;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.api.common.functions.FilterFunction;

public class MaximumHighQualityTemperaturePerYear {

  static int YEAR_FIELD = 0;
  static int MONTH_FIELD = 1;
  static int TEMPERATURE_FIELD = 2;
  static int QUALITY_FIELD = 3;

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    final double qualityThreshold = 0.99;

    DataSet<Tuple4<Short, Short, Integer, Double>> measurements =
        env.readCsvFile(Config.pathTo("temperatures.tsv"))
            .fieldDelimiter('\t')
            .types(Short.class, Short.class, Integer.class, Double.class);


    DataSet<Tuple4<Short, Short, Integer, Double>> highQualityMeasurements =
              // filter orders
              measurements.filter(
                      new FilterFunction<Tuple4<Short, Short, Integer, Double>>() {
                          @Override
                          public boolean filter(Tuple4<Short, Short, Integer, Double> t) {
                              // status filter
                              if(t.f3 >= qualityThreshold) {
                                  return true;
                              }
                              return false;
                          }
                      });



      highQualityMeasurements.print();

    DataSet<Tuple2<Short, Integer>> maxTemperatures =
        highQualityMeasurements
            .groupBy(YEAR_FIELD)
            .aggregate(Aggregations.MAX, TEMPERATURE_FIELD)
            .project(YEAR_FIELD, TEMPERATURE_FIELD).types(Short.class, Integer.class);

    maxTemperatures.writeAsCsv(Config.outputPathTo("maximumHighQualityTemperatures"), FileSystem.WriteMode.OVERWRITE);


    env.execute();
  }

}
