package dataframe;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ProductsSQL {
    public static void main(String[] args) throws AnalysisException {
        SparkSession ss = SparkSession.builder().appName("Products SQL").master("local[*]").getOrCreate();

        Dataset<Row> df = ss.read().option("multiline", true).json("src/main/resources/products.json");
        //df.show();
        //df.printSchema();

        //df.select("name").show();
        //df.select(col("name").alias("Products Name")).show();
        //df.orderBy(col("name").asc()).show();
        //df.filter("name like 'Headphones' and price>50").show();

        df.createTempView("products");
        ss.sql("select * from products where name like 'Headphones'").show();

        //ss.stop();
    }
}
