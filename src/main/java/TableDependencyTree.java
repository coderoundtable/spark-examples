import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.SparkSqlParser;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class TableDependencyTree {
    // Global map to store dependencies between tables
    static Map<String, Set<String>> dependencies = new HashMap<>();

    public static void main(String[] args) throws IOException {
        System.setProperty("hadoop.home.dir", "/");

        // Initialize Spark session
//        SparkSession spark = SparkSession.builder().appName("SparkSQLDependency").master("local").getOrCreate();

        // Retrieve all table dependencies from the directory of SQL files
        retrieveTableDependenciesFromDir();

        // Create a reverse dependency map to get the impact of any table
        Map<String, List<String>> reverseDependencies = createReverseDependencyMap(dependencies);

        // Example: Get the impact of a specific table (e.g., "orders")
        String targetTable = "age_grt_22";  // Replace with your table of interest
//        Set<String> tableImpact = getImpact(targetTable, reverseDependencies);
//
//        // Output the impact
//        System.out.println("Impact for table: " + targetTable);
//        System.out.println(tableImpact);

        // Build and print the dependency tree
        Node root = new Node(targetTable);
        buildDependencyTree(root, dependencies);
        try (PrintWriter out = new PrintWriter("sql-dependency-tree.txt")) {
            out.println(root.prettyPrint());
        }
    }

    // Build the table dependency tree
    public static void buildDependencyTree(Node node, Map<String, Set<String>> dependencies) {
        Set<String> directDependencies = dependencies.get(node.name);
        if (directDependencies != null) {
            for (String dependency : directDependencies) {
                Node child = new Node(dependency);
                node.children.add(child);
                buildDependencyTree(child, dependencies);
            }
        }
    }

    // Function to parse all SQL files and extract table dependencies
    public static void retrieveTableDependenciesFromDir() throws IOException {
        SparkSqlParser parser = new SparkSqlParser();

        Path dir = Paths.get("src/main/resources/sqls/");  // Directory with SQL files
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.sql")) {
            for (Path entry : stream) {
                String sql = new String(Files.readAllBytes(entry));  // Read SQL file content
                Set<String> tableNames = extractTableNames(sql, parser);  // Extract table names

                // Assuming the SQL file represents a view or query, use first table name as "parent"
                String parentTable = tableNames.isEmpty() ? entry.getFileName().toString().replace(".sql", "") : tableNames.iterator().next();

                // Add extracted tables as dependencies
                tableNames.remove(parentTable);  // Avoid self-dependency
                dependencies.putIfAbsent(parentTable, new HashSet<>());
                dependencies.get(parentTable).addAll(tableNames);
            }
        }
    }

    // Extract table names from a given SQL query
    public static Set<String> extractTableNames(String sql, SparkSqlParser parser) {
        Set<String> tableNames = new HashSet<>();

        try {
            // Parse SQL to LogicalPlan
            LogicalPlan plan = parser.parsePlan(sql);

            // Collect table names from UnresolvedRelation nodes (SELECT, JOIN, etc.)
            tableNames.addAll(JavaConverters.seqAsJavaListConverter(plan.collectLeaves()).asJava()
                    .stream()
                    .filter(node -> node instanceof UnresolvedRelation)
                    .map(node -> ((UnresolvedRelation) node).multipartIdentifier().mkString("."))
                    .collect(Collectors.toSet()));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return tableNames;
    }

    // Create reverse dependency map (i.e., tables depending on a given table)
    public static Map<String, List<String>> createReverseDependencyMap(Map<String, Set<String>> dependencies) {
        Map<String, List<String>> reverseDependencies = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : dependencies.entrySet()) {
            String parentTable = entry.getKey();
            Set<String> childTables = entry.getValue();
            for (String table : childTables) {
                reverseDependencies.computeIfAbsent(table, k -> new ArrayList<>()).add(parentTable);
            }
        }
        return reverseDependencies;
    }

    // Get the impact of a table (all dependent tables)
    public static Set<String> getImpact(String table, Map<String, List<String>> reverseDependencies) {
        Set<String> impactSet = new HashSet<>();
        collectImpact(table, reverseDependencies, impactSet);
        return impactSet;
    }

    // Recursive function to collect all dependent tables
    private static void collectImpact(String table, Map<String, List<String>> reverseDependencies, Set<String> impactSet) {
        List<String> impactedTables = reverseDependencies.get(table);
        if (impactedTables != null) {
            for (String impactedTable : impactedTables) {
                if (!impactSet.contains(impactedTable)) {
                    impactSet.add(impactedTable);
                    collectImpact(impactedTable, reverseDependencies, impactSet);
                }
            }
        }
    }

    // Node class for building the dependency tree
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
                builder.append(children.get(children.size() - 1).prettyPrint(prefix + (isTail ? "    " : "│   "), true));
            }
            return builder.toString();
        }
    }
}
