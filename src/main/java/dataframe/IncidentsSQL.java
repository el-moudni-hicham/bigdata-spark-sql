package dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.year;

public class IncidentsSQL {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().appName("Incidents SQL").master("local[*]").getOrCreate();
        Dataset<Row> df = ss.read().option("header", true).option("inferSchema", true).csv("src/main/resources/incidents.csv");

        //df.show();

        Dataset<Row> df1 = df.groupBy("Service").count();
        df1.select(col("Service"), col("count").alias("Incidents Count")).show();

        df.groupBy(year(col("Date")).alias("year")).count().orderBy(col("count").desc()).limit(2).show();
    }
}
