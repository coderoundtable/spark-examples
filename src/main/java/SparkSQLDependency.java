import com.google.gson.Gson;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.execution.SparkSqlParser;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class SparkSQLDependency {
    // Map to store dependencies
    static Map<String, List<String>> dependencies = new HashMap<>();

    public static void main(String[] args) throws IOException, ParseException {

        System.setProperty("hadoop.home.dir", "/");
        // Initialize Spark session
        SparkSession spark = SparkSession.builder().master("local").appName("SQL Dependency Parser").getOrCreate();

        // Assume dependencies are populated
        Map<String, List<String>> dependencies =  retrieveDependenciesFromDir(spark);
        String viewName = "aggregate_view"; // replace with your view name
        Node root = new Node(viewName);
        buildDependencyTree(root, dependencies);

        // Write output to a file
        try (PrintWriter out = new PrintWriter("sql-dependency-tree.txt")) {
            out.println(root.prettyPrint());
        }
    }

    public static void buildDependencyTree(Node node, Map<String, List<String>> dependencies) {
        List<String> directDependencies = dependencies.get(node.name);
        if (directDependencies != null) {
            for (String dependency : directDependencies) {
                Node child = new Node(dependency);
                node.children.add(child);
                buildDependencyTree(child, dependencies);
            }
        }
    }

    public static Map<String, List<String>> retrieveDependencies(SparkSession spark) throws IOException, ParseException {
        // Initialize Spark SQL parser
        SparkSqlParser parser = new SparkSqlParser();

        // Load fromJson from JSON
        String json = new String(Files.readAllBytes(Paths.get("src/main/resources/dependencies.json")));
        Map<String, Object> fromJson = new Gson().fromJson(json, Map.class);

        // Parse SQL files to find dependencies
        Map<String, List<String>> dependencies = new HashMap<>();
        for (String queryName : ((Map<String, Double>) fromJson.get("queries")).keySet()) {
            String sql = new String(Files.readAllBytes(Paths.get("src/main/resources/sqls/" + queryName + ".sql")));

            // Parse SQL using Spark SQL parser
            LogicalPlan plan = parser.parsePlan(sql);

            // Extract table names
            List<String> tableList = JavaConverters.seqAsJavaListConverter(plan.collectLeaves()).asJava()
                    .stream()
                    .filter(node -> node instanceof UnresolvedRelation)
                    .map(node -> ((UnresolvedRelation) node).tableName())
                    .collect(Collectors.toList());

            // Add dependencies to the map
            dependencies.put(queryName, tableList);
        }
        return dependencies;
    }

    public static Map<String, List<String>> retrieveDependenciesFromDir(SparkSession spark) throws IOException, ParseException {
        // Initialize Spark SQL parser
        SparkSqlParser parser = new SparkSqlParser();

        // Parse SQL files to find dependencies
        Map<String, List<String>> dependencies = new HashMap<>();
        Path dir = Paths.get("src/main/resources/sqls/");
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.sql")) {
            for (Path entry : stream) {
                String queryName = entry.getFileName().toString().replace(".sql", "");
                String sql = new String(Files.readAllBytes(entry));

                // Parse SQL using Spark SQL parser
                LogicalPlan plan = parser.parsePlan(sql);

                // Extract table names
                List<String> tableList = JavaConverters.seqAsJavaListConverter(plan.collectLeaves()).asJava()
                        .stream()
                        .filter(node -> node instanceof UnresolvedRelation)
                        .map(node -> ((UnresolvedRelation) node).tableName())
                        .collect(Collectors.toList());

                // Add dependencies to the map
                dependencies.put(queryName, tableList);
            }
        }
        return dependencies;
    }

    static class Node {
        String name;
        List<Node> children = new ArrayList<>();

        Node(String name) {
            this.name = name;
        }

        String prettyPrint() {
            return prettyPrint("", true);
        }

        private String prettyPrint(String prefix, boolean isTail) {
            StringBuilder builder = new StringBuilder();

            builder.append(prefix).append(isTail ? "└── " : "├── ").append(name).append("\n");
            for (int i = 0; i < children.size() - 1; i++) {
                builder.append(children.get(i).prettyPrint(prefix + (isTail ? "    " : "│   "), false));
            }
            if (children.size() > 0) {
                builder.append(children.get(children.size() - 1).prettyPrint(prefix + (isTail ?"    " : "│   "), true));
            }

            return builder.toString();
        }
    }
}
