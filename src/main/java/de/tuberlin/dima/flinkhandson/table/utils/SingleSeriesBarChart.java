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

package de.tuberlin.dima.flinkhandson.table.utils;

import java.awt.Color;
import java.awt.Dimension;
import java.util.Map;
import javax.swing.JFrame;

import org.jfree.chart.*;
import org.jfree.chart.axis.*;
import org.jfree.chart.plot.*;
import org.jfree.chart.renderer.category.*;
import org.jfree.data.category.DefaultCategoryDataset;

public class SingleSeriesBarChart extends JFrame {
	
		public SingleSeriesBarChart(String title, String xLabel, String yLabel, Color barColor, Map<String,Double> result) {
			
			DefaultCategoryDataset dataset = new DefaultCategoryDataset();
			for(Map.Entry<String,Double> e : result.entrySet()) {
				dataset.addValue(e.getValue(), "", e.getKey());
			}

			JFreeChart chart = ChartFactory.createBarChart(
					title,
					xLabel,
					yLabel,
					dataset,
					PlotOrientation.VERTICAL,
					false,
					true,
					false
			);

			CategoryPlot plot = (CategoryPlot) chart.getPlot();
			CategoryAxis xAxis = plot.getDomainAxis();

			xAxis.setCategoryLabelPositions(CategoryLabelPositions.DOWN_45);
			plot.setRangeGridlinesVisible(true);
			plot.setRangeGridlinePaint(Color.BLACK);
			plot.setBackgroundPaint(Color.WHITE);

			BarRenderer renderer = (BarRenderer) plot.getRenderer();
			renderer.setBarPainter(new StandardBarPainter());

			renderer.setSeriesPaint(0, barColor);

			ChartPanel chartPanel = new ChartPanel(chart);
			chartPanel.setPreferredSize(new Dimension(1024, 768));
			getContentPane().add(chartPanel);

			pack();
			setVisible(true);
			setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		}

}
