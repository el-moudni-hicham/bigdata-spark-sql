import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AppDB {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().appName("MySQL").master("local[*]").getOrCreate();

        Dataset<Row> df = ss.read().format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/db_name")
                .option("user","root")
                .option("password","")
                //.option("dtable","PRODUCTS")
                .option("query","select * from PRODUCTS")
                .load();
        df.show();
    }
}
