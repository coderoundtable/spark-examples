package util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class SparkUtil {
        //write views to s3 if there is a need to write the view to a s3 bucket
        public static void writeViewsToS3(SparkSession sparkSession, List<String> writeToS3) {
            for (String viewName : writeToS3) {
                Dataset<Row> df = sparkSession.table(viewName);
                df.write().parquet("s3a://bucket-name/" + viewName);
            }
        }
        public static void writeViewsToTables(SparkSession sparkSession, List<String> writeToTable) {
            for (String viewName : writeToTable) {
                Dataset<Row> df = sparkSession.table(viewName);
                df.show(false);
                //df.write().saveAsTable(viewName);
            }
        }

        //create method to get dataset from view
        public static Dataset<Row> getDatasetFromView(SparkSession sparkSession, String viewName) {
            return sparkSession.table(viewName);
        }

        public static void loadCSV(SparkSession sparkSession, String path, String tableName) {
            Dataset<Row> df = sparkSession.read().option("header", "true").csv(path);
            df.createOrReplaceTempView(tableName);
        }
}
