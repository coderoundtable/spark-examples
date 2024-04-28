package runners;


import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import util.JSONReader;

import java.io.IOException;
import java.util.*;

public class QueryRunner {
    private SparkSession spark;
    private Map<String, String> queries;  // Map of view name to SQL query
    private Map<String, List<String>> dependencies;  // Map of view name to a list of dependencies
    private List<String> writeToTable;  // List of views to write to tables

    public QueryRunner(SparkSession spark, JSONReader jsonReader) throws IOException {
        this.spark = spark;
        this.queries = jsonReader.getQueries();
        this.dependencies = jsonReader.getDependencies();
        this.writeToTable = jsonReader.getWriteToTable();

    }
    //run query by view name and create a view
    public void runQuery(String viewName) {

        // Check if the spark view is already created
        if (spark.catalog().tableExists(viewName)) {
            System.out.println("View already exists: " + viewName);
            return;
        }

        // Run dependencies first
        if (dependencies.containsKey(viewName)) {
            for (String dependency : dependencies.get(viewName)) {
                System.out.println("Running dependency: " + dependency + " for view: " + viewName);
                runQuery(dependency);
            }
        }


        System.out.println("Running query for view: " + viewName);
        Dataset<Row> df = spark.sql(queries.get(viewName));

        // Create a view
        df.createOrReplaceTempView(viewName);

    }

    public void runAllQueries() {
        for (String viewName : queries.keySet()) {
            runQuery(viewName);
        }
    }

}
