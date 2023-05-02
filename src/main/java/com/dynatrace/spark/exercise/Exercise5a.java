package com.dynatrace.spark.exercise;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class Exercise5a {

    private static final String VALUE = "value";
    private static final String FILE_NAME = "partition";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("WordCount")
                .setMaster("local[*]")
                .set("spark.driver.bindAddress", "127.0.0.1");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");

        StructType schema = DataTypes.createStructType(new StructField[]{
                new StructField(VALUE, DataTypes.StringType, false, Metadata.empty()),
                new StructField(FILE_NAME, DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> combined = sparkSession.emptyDataset(RowEncoder.apply(schema));

        File dir = new File("./data/testExample");
        File[] files = dir.listFiles();
        if (files == null) {
            return;
        }

        for (int i = 0; i < files.length; i++) {
            int index = i;
            Dataset<Row> d = sparkSession.read().textFile(files[i].getAbsolutePath())
                    .flatMap((FlatMapFunction<String, String>) s ->
                            Arrays.stream(s.toLowerCase().split("[^a-zA-Z\\d]")).iterator(), Encoders.STRING())
                    .map((MapFunction<String, Row>) x -> RowFactory.create(x, files[index].getName()), RowEncoder.apply(schema));
            combined = combined.union(d);
        }

        combined = combined.repartitionByRange(files.length, col(FILE_NAME));
        combined.withColumn("spark partition", spark_partition_id()).show();

        Dataset<Row> result = combined
                .select(col(VALUE))
                .filter(col(VALUE).notEqual(""))
                .groupBy(VALUE)
                .count()
                .sort(col("count").desc());
        result.show();
    }

}
