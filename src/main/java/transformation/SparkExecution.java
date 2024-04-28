package transformation;

import org.apache.spark.sql.SparkSession;
import runners.QueryRunner;
import util.SparkUtil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkExecution {
    public static void main(String[] args) {
        // Initialize Spark Session
        System.setProperty("hadoop.home.dir", "/");
        SparkSession spark = SparkSession.builder().master("local").appName("QueryRunnerExample").getOrCreate();
        try {

            String jsonPath = "src/main/resources/queries-dependencies.json";

            //load all csv files and create views
            SparkUtil.loadCSV(spark, "src/main/resources/customers-100.csv", "customers");

            // Create a QueryRunner object
            QueryRunner queryRunner = new QueryRunner(spark, jsonPath);

            // Run all queries
            queryRunner.runAllQueries();

            // Write specific views to tables (if needed)

            List<String> viewsToWrite = queryRunner.getWriteToTable();
            SparkUtil.writeViewsToTables(spark, viewsToWrite);

        } catch (Exception e) {
            e.printStackTrace();

            // Stop the Spark Session
            spark.stop();
        }
    }
}

