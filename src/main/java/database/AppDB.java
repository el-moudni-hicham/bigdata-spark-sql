package database;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.col;


public class AppDB {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().appName("MySQL").master("local[*]").getOrCreate();

        Map<String, String> option = new HashMap<>();
        option.put("driver", "com.mysql.jdbc.Driver");
        option.put("url", "jdbc:mysql://localhost:3306/db_hopital_spark");
        option.put("user","root");
        option.put("password","");

        Dataset<Row> df1 = ss.read().format("jdbc")
                .options(option)
                //.option("table","table_name")
                .option("query","select * from consultations")
                .load();
        Dataset<Row> df2 = ss.read().format("jdbc")
                .options(option)
                .option("query","select * from medecins")
                .load();

        //df1.show();
        df1.groupBy(col("DATE_CONSULTATION").alias("day")).count().show();




        Dataset<Row> dfConsultations = df1.groupBy(col("id_medecin")).count();
        Dataset<Row> dfMedicins = df2.select("id","nom","prenom");

        Dataset<Row> joinedDF = dfMedicins
                .join(dfConsultations, dfMedicins.col("id").equalTo(dfConsultations.col("id_medecin")), "inner")
                .select(dfMedicins.col("nom"), dfMedicins.col("prenom"), dfConsultations.col("count").alias("NOMBRE DE CONSULTATION"))
                .orderBy(col("NOMBRE DE CONSULTATION").desc());

        joinedDF.show();




    }
}
