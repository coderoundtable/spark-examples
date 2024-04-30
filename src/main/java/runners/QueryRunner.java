package runners;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import util.JSONReader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryRunner {
    private SparkSession spark;
    private Map<String, String> queries;  // Map of view name to SQL query
    private Map<String, List<String>> dependencies;  // Map of view name to a list of dependencies
    private List<String> writeToTable;  // List of views to write to tables
    private String sqlFilesDirectory;

    public QueryRunner(SparkSession spark, JSONReader jsonReader) throws IOException {
        this.spark = spark;
        this.queries = jsonReader.getQueries();
        this.dependencies = jsonReader.getDependencies();
        this.writeToTable = jsonReader.getWriteToTable();

    }

    public QueryRunner(SparkSession spark, String sqlFilesDirectory,  JSONReader jsonReader) throws IOException {
        this.spark = spark;
        this.queries = loadQueriesFromFiles(sqlFilesDirectory);
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

    private Map<String, String> loadQueriesFromFiles(String sqlFilesDirectory) {
        Map<String, String> loadedQueries = new HashMap<>();
        File directory = new File(sqlFilesDirectory);
        if (directory.isDirectory()) {
            for (File file : directory.listFiles()) {
                if (file.isFile() && file.getName().endsWith(".sql")) {
                    try {
                        String query = new String(Files.readAllBytes(Paths.get(file.getAbsolutePath())));
                        loadedQueries.put(file.getName().replace(".sql", ""), query);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return loadedQueries;
    }

}
