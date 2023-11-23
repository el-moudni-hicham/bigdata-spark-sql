package dataset;

import dataset.entites.Product;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class ProductDS {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder()
                .appName("products dataset")
                .master("local[*]")
                .getOrCreate();

        Product p1 = new Product("Book Java", 250.5, 20);
        Product p2 = new Product("Book Python", 150.5, 20);

        Encoder<Product> productEncoder = Encoders.bean(Product.class);
        Dataset<Product> ds = ss.createDataset(
                Arrays.asList(p1, p2), productEncoder
        );

        ds.show();
    }
}
