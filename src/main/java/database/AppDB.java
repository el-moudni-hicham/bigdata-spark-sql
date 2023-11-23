package database;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;


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


        // q1
        df1.groupBy(col("DATE_CONSULTATION").alias("day")).count().show();



        // q2
        Dataset<Row> dfConsultations = df1.groupBy(col("id_medecin")).count();
        Dataset<Row> dfMedicins = df2.select("id","nom","prenom");

        Dataset<Row> joinedDF = dfMedicins
                .join(dfConsultations, dfMedicins.col("id").equalTo(dfConsultations.col("id_medecin")), "inner")
                .select(dfMedicins.col("nom"), dfMedicins.col("prenom"), dfConsultations.col("count").alias("NOMBRE DE CONSULTATION"))
                .orderBy(col("NOMBRE DE CONSULTATION").desc());

        joinedDF.withColumnRenamed("nom","NOM");
        joinedDF.withColumnRenamed("prenom","PRENOM");

        joinedDF.show();

        // q3

        Dataset<Row> dfMedPat = df1.select("id_medecin", "id_patient");
        dfMedPat.groupBy(col("id_medecin").alias("Medecin")).agg(countDistinct("id_patient").as("Number of patients")).show();
    }
}
