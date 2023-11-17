package database;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class AppDB {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().appName("MySQL").master("local[*]").getOrCreate();

        Dataset<Row> df = ss.read().format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/db_hopital_spark")
                .option("user","root")
                .option("password","")
                //.option("dtable","PRODUCTS")
                .option("query","select * from consultations")
                .load();

        //df.show();
        df.groupBy(dayofmonth(col("DATE_CONSULTATION")).alias("day")).count().show();
    }
}
