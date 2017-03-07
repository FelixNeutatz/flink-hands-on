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

package de.tuberlin.dima.flinkhandson.matrices;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;


public class TraceOfSum {

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    /*
         1 1 3       1 0 0
      A  1 2 0    B  0 1 3
         0 1 1       2 0 5
    */

    DataSource<Cell> matrixA =
        env.fromElements(new Cell(0, 0, 1), new Cell(0, 1, 1), new Cell(0, 2, 3), new Cell(1, 0, 1), new Cell(1, 1, 2),
            new Cell(2, 1, 1), new Cell(2, 2, 1));

    DataSource<Cell> matrixB =
        env.fromElements(new Cell(0, 0, 1), new Cell(1, 1, 1), new Cell(1, 2, 3), new Cell(2, 0, 2), new Cell(2, 2, 5));

    // IMPLEMENT THIS STEP
    DataSet<Double> trace = matrixA.union(matrixB).flatMap(new ValueExtraction()).aggregate(Aggregations.SUM, 0).flatMap(new TupleToDouble());;

    trace.print();

    env.execute();

  }


    // User-defined functions
    public static final class ValueExtraction implements FlatMapFunction<Cell, Tuple1<Double>> {

        @Override
        public void flatMap(Cell value, Collector<Tuple1<Double>> out) {

            if (value.i == value.j) {
                out.collect(new Tuple1<Double>(value.value));
            } else {
                out.collect(new Tuple1<Double>(0.0));
            }

        }
    }

    // User-defined functions
    public static final class TupleToDouble implements FlatMapFunction<Tuple1<Double>, Double> {

        @Override
        public void flatMap(Tuple1<Double> value, Collector<Double> out) {
            out.collect(value.f0);

        }
    }

}
