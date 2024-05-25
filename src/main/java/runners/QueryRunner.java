package runners;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import util.JSONReader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class QueryRunner {
    private SparkSession spark;
    private Map<String, Integer> queries;  // Map of view name to level
    private Map<String, String> sqlQueries;  // Map of view name to SQL query
    private Map<String, List<String>> dependencies;  // Map of view name to a list of dependencies
    private List<String> writeToTable;  // List of views to write to tables
    private String sqlFilesDirectory;

    public QueryRunner(SparkSession spark, JSONReader jsonReader, String sqlFilesDirectory) throws IOException {
        this.spark = spark;
        this.queries = jsonReader.getQueries();
        this.dependencies = jsonReader.getDependencies();
        this.writeToTable = jsonReader.getWriteToTable();
        this.sqlFilesDirectory = sqlFilesDirectory;
        this.sqlQueries = loadAllQueriesFromFiles();
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
        String query = sqlQueries.get(viewName);
        Dataset<Row> df = spark.sql(query);

        // Create a view
        df.createOrReplaceTempView(viewName);

    }

    public void runAllQueries() {
        // Sort the queries by their level
        List<Map.Entry<String, Integer>> sortedQueries = new ArrayList<>(queries.entrySet());
        sortedQueries.sort(Comparator.comparingInt(Map.Entry::getValue));

        // Execute the queries in order
        for (Map.Entry<String, Integer> entry : sortedQueries) {
            runQuery(entry.getKey());
        }
    }

    private Map<String, String> loadAllQueriesFromFiles() {
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
