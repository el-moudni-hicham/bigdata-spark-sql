# Big Data Spark SQL


```
This project includes a brief but informative and simple explanation of Apache Spark and
Spark SQL terms with java implementation. There are few structured examples to clear
the concept and terms in Apache Spark and Spark SQL altogether.
```

## Table of Contents
1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Applications](#applications)
   * [Products SQL](#products-sql)
   * [Incidents SQL](#incidents-sql)
   * [Hopital Database](#hopital-database)


## Overview

### Spark SQL

Spark SQL is a Spark module for structured data processing. Unlike the basic Spark RDD API, the interfaces provided by Spark SQL provide Spark with more information about the structure of both the data and the computation being performed. Internally, Spark SQL uses this extra information to perform extra optimizations.

## Prerequisites

 * Spark SQL

   Add this maven dependencies to pom.xml file:
   
   ```maven
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.13</artifactId>
            <version>3.4.1</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.13</artifactId>
            <version>3.4.1</version>
        </dependency>
   ```

 * Java

## Applications

### Products SQL

We want to develop a Spark application for Products.

The products are stored in a CSV file.

![image](https://github.com/el-moudni-hicham/bigdata-spark-sql/assets/85403056/5a8c3ffa-3748-4dce-b16c-051d1029f8b6)

```java
SparkSession ss = SparkSession.builder().appName("Products SQL").master("local[*]").getOrCreate();
Dataset<Row> df = ss.read().option("multiline", true).json("src/main/resources/products.json");
```

```java
df.show();
```

![image](https://github.com/el-moudni-hicham/bigdata-spark-sql/assets/85403056/736c66c5-a3ce-446e-83e0-297276e532fb)


```java
df.printSchema();
```

![image](https://github.com/el-moudni-hicham/bigdata-spark-sql/assets/85403056/fc9c279b-731f-499f-b0a8-aa016576bf0d)

```java
df.select("name").show();        
```

![image](https://github.com/el-moudni-hicham/bigdata-spark-sql/assets/85403056/2aae07ec-36da-474a-b26b-002b3b20c944)


```java
df.select(col("name").alias("Products Name")).show();
```

![image](https://github.com/el-moudni-hicham/bigdata-spark-sql/assets/85403056/782d2ee3-0f14-4f9f-9b3e-c86bf081bfa3)


```java
df.orderBy(col("name").asc()).show();
```

![image](https://github.com/el-moudni-hicham/bigdata-spark-sql/assets/85403056/6f68a29b-aab4-4a70-b009-fc2a3d74b1e0)


```java
df.filter("name like 'Headphones' and price>50").show();
```

![image](https://github.com/el-moudni-hicham/bigdata-spark-sql/assets/85403056/7057e067-ce9b-4566-8785-7ea9f678d340)


```java
df.createTempView("products");
ss.sql("select * from products where name like 'Headphones'").show();
```

![image](https://github.com/el-moudni-hicham/bigdata-spark-sql/assets/85403056/d5c1837d-dca5-4ab1-a63d-613f9f8ab234)


### Incidents SQL
We want to develop a Spark application for an industrial company that processes incidents from each service. 

The incidents are stored in a CSV file.

![image](https://github.com/el-moudni-hicham/bigdata-spark-sql/assets/85403056/a0119451-b944-4662-a07a-3ce56b89fa6e)

```java
SparkSession ss = SparkSession.builder().appName("Incidents SQL").master("local[*]").getOrCreate();
Dataset<Row> df = ss.read().option("header", true).option("inferSchema", true).csv("src/main/resources/incidents.csv");
```

* Display the number of incidents per service.

```java
Dataset<Row> df1 = df.groupBy("Service").count();
df1.select(col("Service"), col("count").alias("Incidents Count")).show();
```

![image](https://github.com/el-moudni-hicham/bigdata-spark-sql/assets/85403056/34972b5f-83c0-4720-8186-60733427a6cb)

* Display the two years with the most incidents.

```java
df.groupBy(year(col("Date")).alias("year")).count().orderBy(col("count").desc()).limit(2).show();
```

![image](https://github.com/el-moudni-hicham/bigdata-spark-sql/assets/85403056/80354378-c621-45ac-8f09-a12ea7eed294)

### Hopital Database

 Create MySQL database named DB_HOPITAL, which contains three tables 
 
 * PATIENTS :

![image](https://github.com/el-moudni-hicham/bigdata-spark-sql/assets/85403056/12cd8ab1-d6d5-4f85-b1d7-5162608c6b83)

 * MEDECINS :

![image](https://github.com/el-moudni-hicham/bigdata-spark-sql/assets/85403056/23e115fb-f9ed-42a0-a1b8-5d6ce0fe332c)

 * CONSULTATIONS :

![image](https://github.com/el-moudni-hicham/bigdata-spark-sql/assets/85403056/3a488794-074a-4189-89bf-7969e8a8674b)

* Get access to data in database :
  
```java
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
```

* Display the number of consultations per day :

```java
df1.groupBy(col("DATE_CONSULTATION").alias("day")).count().show();
```

![image](https://github.com/el-moudni-hicham/bigdata-spark-sql/assets/85403056/9dfd46f1-5606-46d7-91d8-b24a300b631c)


* Display the number of consultations per doctor. The display format is as follows:

`NAME | SURNAME | NUMBER OF CONSULTATIONS`

```java
        Dataset<Row> dfConsultations = df1.groupBy(col("id_medecin")).count();
        Dataset<Row> dfMedicins = df2.select("id","nom","prenom");

        Dataset<Row> joinedDF = dfMedicins
                .join(dfConsultations, dfMedicins.col("id").equalTo(dfConsultations.col("id_medecin")), "inner")
                .select(dfMedicins.col("nom"), dfMedicins.col("prenom"),
                 dfConsultations.col("count").alias("NOMBRE DE CONSULTATION"))
                .orderBy(col("NOMBRE DE CONSULTATION").desc());

        joinedDF.show();
```

![image](https://github.com/el-moudni-hicham/bigdata-spark-sql/assets/85403056/a20eaf6d-cdaf-4864-a2e9-8122f7724aab)

