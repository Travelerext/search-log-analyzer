package com.sohu.logs.util;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.labels.StandardPieSectionLabelGenerator;
import org.jfree.chart.plot.PiePlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.general.DefaultPieDataset;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ChartGenerator {
    
    static {
        System.setProperty("java.awt.headless", "true");
        

        try {
            java.awt.Font font = new java.awt.Font("Microsoft YaHei", java.awt.Font.PLAIN, 12);
            if (!font.getFontName().contains("YaHei")) {
                font = new java.awt.Font("SimSun", java.awt.Font.PLAIN, 12);
                if (!font.getFontName().contains("SimSun")) {
                    font = new java.awt.Font("SansSerif", java.awt.Font.PLAIN, 12);
                }
            }

            org.jfree.chart.StandardChartTheme chartTheme = new org.jfree.chart.StandardChartTheme("CN");
            chartTheme.setExtraLargeFont(font.deriveFont(20f));
            chartTheme.setLargeFont(font.deriveFont(16f));
            chartTheme.setRegularFont(font.deriveFont(14f));
            chartTheme.setSmallFont(font.deriveFont(12f));
            org.jfree.chart.ChartFactory.setChartTheme(chartTheme);
        } catch (Exception e) {
            System.err.println("设置中文字体失败，图表可能显示乱码: " + e.getMessage());
        }
    }
    
    public static void generateBarChart(Dataset<Row> data, String title, 
                                       String categoryColumn, String valueColumn,
                                       String outputPath, int width, int height) throws IOException {
        if (data.isEmpty()) {
            System.err.println("警告: 数据集为空，跳过图表生成: " + title);
            return;
        }
        
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        
        List<Row> rows = data.collectAsList();
        for (Row row : rows) {
            String category = row.getAs(categoryColumn).toString();
            Number value = row.getAs(valueColumn);
            dataset.addValue(value, "Value", category);
        }
        
        JFreeChart chart = ChartFactory.createBarChart(
            title,
            categoryColumn,
            valueColumn,
            dataset,
            PlotOrientation.VERTICAL,
            true,
            true,
            false
        );
        
        File outputFile = new File(outputPath);
        ChartUtils.saveChartAsPNG(outputFile, chart, width, height);
    }
    
    public static void generateTop8BarChartWithOther(Dataset<Row> data, String title,
                                                     String categoryColumn, String valueColumn,
                                                     String outputPath, int width, int height) throws IOException {
        if (data.isEmpty()) {
            System.err.println("警告: 数据集为空，跳过图表生成: " + title);
            return;
        }
        
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        
        List<Row> rows = data.collectAsList();
        int totalRows = rows.size();
        
        if (totalRows > 8) {
            double otherSum = 0.0;
            for (int i = 0; i < rows.size(); i++) {
                Row row = rows.get(i);
                String category = row.getAs(categoryColumn).toString();
                Number value = row.getAs(valueColumn);
                
                if (i < 8) {
                    dataset.addValue(value, "Value", category);
                } else {
                    otherSum += value.doubleValue();
                }
            }
            
            if (otherSum > 0) {
                dataset.addValue(otherSum, "Value", "其他");
            }
        } else {
            for (Row row : rows) {
                String category = row.getAs(categoryColumn).toString();
                Number value = row.getAs(valueColumn);
                dataset.addValue(value, "Value", category);
            }
        }
        
        JFreeChart chart = ChartFactory.createBarChart(
            title,
            categoryColumn,
            valueColumn,
            dataset,
            PlotOrientation.VERTICAL,
            true,
            true,
            false
        );
        
        File outputFile = new File(outputPath);
        ChartUtils.saveChartAsPNG(outputFile, chart, width, height);
    }
    

    public static void generateTop8PieChartWithOther(Dataset<Row> data, String title,
                                                    String categoryColumn, String valueColumn,
                                                    String outputPath, int width, int height) throws IOException {
        if (data.isEmpty()) {
            System.err.println("警告: 数据集为空，跳过图表生成: " + title);
            return;
        }
        
        DefaultPieDataset dataset = new DefaultPieDataset();
        
        List<Row> rows = data.collectAsList();
        int totalRows = rows.size();
        
        if (totalRows > 8) {
            double otherSum = 0.0;
            for (int i = 0; i < rows.size(); i++) {
                Row row = rows.get(i);
                String category = row.getAs(categoryColumn).toString();
                Number value = row.getAs(valueColumn);
                
                if (i < 8) {
                    dataset.setValue(category, value);
                } else {
                    otherSum += value.doubleValue();
                }
            }
            
            if (otherSum > 0) {
                dataset.setValue("其他", otherSum);
            }
        } else {
            for (Row row : rows) {
                String category = row.getAs(categoryColumn).toString();
                Number value = row.getAs(valueColumn);
                dataset.setValue(category, value);
            }
        }
        
        JFreeChart chart = ChartFactory.createPieChart(
            title,
            dataset,
            true,
            true,
            false
        );
        
        PiePlot plot = (PiePlot) chart.getPlot();
        StandardPieSectionLabelGenerator labelGenerator = 
            new StandardPieSectionLabelGenerator("{0}: {1} ({2})");
        plot.setLabelGenerator(labelGenerator);
        
        File outputFile = new File(outputPath);
        ChartUtils.saveChartAsPNG(outputFile, chart, width, height);
    }
    

    
    public static void generateTop8HorizontalBarChartWithOther(Dataset<Row> data, String title,
                                                              String categoryColumn, String valueColumn,
                                                              String outputPath, int width, int height) throws IOException {
        if (data.isEmpty()) {
            System.err.println("警告: 数据集为空，跳过图表生成: " + title);
            return;
        }
        
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        
        List<Row> rows = data.collectAsList();
        int totalRows = rows.size();
        
        if (totalRows > 8) {
            double otherSum = 0.0;
            for (int i = 0; i < rows.size(); i++) {
                Row row = rows.get(i);
                String category = row.getAs(categoryColumn).toString();
                Number value = row.getAs(valueColumn);
                
                if (i < 8) {
                    dataset.addValue(value, "Value", category);
                } else {
                    otherSum += value.doubleValue();
                }
            }
            
            if (otherSum > 0) {
                dataset.addValue(otherSum, "Value", "其他");
            }
        } else {
            for (Row row : rows) {
                String category = row.getAs(categoryColumn).toString();
                Number value = row.getAs(valueColumn);
                dataset.addValue(value, "Value", category);
            }
        }
        
        JFreeChart chart = ChartFactory.createBarChart(
            title,
            categoryColumn,
            valueColumn,
            dataset,
            PlotOrientation.HORIZONTAL,
            true,
            true,
            false
        );
        
        File outputFile = new File(outputPath);
        ChartUtils.saveChartAsPNG(outputFile, chart, width, height);
    }
    

    

}