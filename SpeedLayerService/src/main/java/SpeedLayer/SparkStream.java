package SpeedLayer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.col;


import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;
import static org.apache.spark.sql.functions.udf;


public class SparkStream extends Thread{
    SparkSession spark;
    StructType subSchema = new StructType()
            .add("Total", DataTypes.FloatType)
            .add("Free", DataTypes.FloatType);

    StructType schema = new StructType().add("serviceName", DataTypes.StringType)
            .add("Timestamp", DataTypes.LongType)
            .add("CPU", DataTypes.FloatType)
            .add("RAM", subSchema)
            .add("Disk", subSchema);
    private String inputPath ;
    private String outputPath;
    private String checkPointPath = "src/main/resources/sparkCheckPoint";
    public SparkStream(String inputPath, String outputPath){
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public void run() {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        spark = SparkSession
                .builder()
                .appName("SparkStream")
                .config("spark.master", "local")
                .config("spark.sql.streaming.checkpointLocation", checkPointPath)
                .getOrCreate();
        Dataset<Row> msgDF = spark
                .readStream()
                .format("json")
                .schema(schema)
                .json(inputPath);
        msgDF.createOrReplaceTempView("record");
        Dataset<Row> sqlDF = spark.sql("SELECT serviceName,Timestamp, CPU ,Disk.Total - Disk.Free AS Disk,RAM.Total - RAM.Free AS RAM FROM record");
//        sqlDF.printSchema();
//        msgDF.printSchema();
        try {
            sqlDF.writeStream()
                    .format("parquet")
                    .option("path", outputPath)
                    .outputMode("append")
                    .start().awaitTermination();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (StreamingQueryException e) {
            throw new RuntimeException(e);
        }

//        msgDF.writeStream()
//                .outputMode("append")
//                .format("console")
//                .start().awaitTermination();
//        sqlDF.writeStream()
//                .outputMode("append")
//                .format("console")
//                .start().awaitTermination();
    }
//    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
//        start();
//    }
}
