package com.sundogsoftware.spark;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.spark.sql.SaveMode;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
public class Converter {

    public static void writeDataFrameToCSV(List<String> data, String filePath) {
        try (CSVPrinter csvPrinter = new CSVPrinter(new FileWriter(filePath), CSVFormat.DEFAULT)) {
            for (String row : data) {
                csvPrinter.printRecord(row);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
//    // Step 1: Convert DataFrame to RDD[Row]
//    val rdd: org.apache.spark.rdd.RDD[Row] = df.rdd
//
//    // Step 2: Convert RDD[Row] to JavaRDD[Row]
//    val javaRDD: JavaRDD[Row] = rdd.toJavaRDD
//
//    // Step 3: Convert JavaRDD[Row] to Java List[Row]
//    val javaList: java.util.List[Row] = javaRDD.collect()