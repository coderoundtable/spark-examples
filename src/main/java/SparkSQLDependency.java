import com.google.gson.Gson;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.execution.SparkSqlParser;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.runtime.AbstractPartialFunction;

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
        SparkSession spark = SparkSession.builder().appName("SparkSQLDependency").master("local").getOrCreate();

        // Assume dependencies are populated for tables
        Map<String, List<String>> dependencies =  retrieveDependenciesFromDir(spark);

        // Assume dependencies are populated for colums
//        Map<String, List<String>> dependencies = retrieveColumnDependenciesFromDir(spark);
        String viewName = "aggregate_view"; // replace with your view name
        Node root = new Node(viewName);


        // Build dependency tree for columns
//        buildColDependencyTree(root, dependencies);

        //build dependency tree for tables
        buildDependencyTree(root, dependencies);

        // Get impact of a table
        List<String> tableImpact = getImpact("customers", createReverseDependencyMap(dependencies));
        System.out.println(tableImpact);

        // Get impact of a column
//        List<String> columnImpact = getImpact("c.first_name", createReverseColumnDependencyMap(dependencies));
//        System.out.println(columnImpact);

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

    public static void buildColDependencyTree(Node node, Map<String, List<String>> dependencies) {
        List<String> directDependencies = dependencies.get(node.name);
        if (directDependencies != null) {
            for (String dependency : directDependencies) {
                Node child = new Node(dependency);
                node.children.add(child);
                buildColDependencyTree(child, dependencies);
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

    public static Map<String, List<String>> retrieveColumnDependenciesFromDir(SparkSession sparkSession) throws IOException, ParseException {
        // Initialize Spark SQL parser
        SparkSqlParser parser = new SparkSqlParser();

        // Parse SQL files to find dependencies
        Map<String, List<String>> dependencies = new HashMap<>();
        Path dir = Paths.get("src/main/resources/sqls/");
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.sql")) {
            for (Path entry : stream) {
                String sql = new String(Files.readAllBytes(entry));

                // Parse SQL using Spark SQL parser
                LogicalPlan plan = parser.parsePlan(sql);


                // Add dependencies to the map
                dependencies.put(entry.getFileName().toString().replace(".sql", ""), getColumnNames(sql,sparkSession));
            }
        }
        return dependencies;
    }

    public static Map<String, List<String>> createReverseDependencyMap(Map<String, List<String>> dependencies) {
        Map<String, List<String>> reverseDependencies = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : dependencies.entrySet()) {
            String query = entry.getKey();
            List<String> tablesOrColumns = entry.getValue();
            for (String tableOrColumn : tablesOrColumns) {
                if (!reverseDependencies.containsKey(tableOrColumn)) {
                    reverseDependencies.put(tableOrColumn, new ArrayList<>());
                }
                reverseDependencies.get(tableOrColumn).add(query);
            }
        }
        return reverseDependencies;
    }

    public static List<String> getImpact(String tableOrColumn, Map<String, List<String>> reverseDependencies) {
        return reverseDependencies.getOrDefault(tableOrColumn, Collections.emptyList());
    }


    public static List<String> getColumnNames(String sql, SparkSession spark) throws ParseException {
        // Parse the SQL query into a LogicalPlan
        LogicalPlan plan = spark.sessionState().sqlParser().parsePlan(sql);

        // Initialize a list to hold the column names
        List<String> columnNames = new ArrayList<>();

        // Traverse the LogicalPlan to find UnresolvedAttributes
        plan.transformAllExpressions(new AbstractPartialFunction<Expression, Expression>() {
            @Override
            public Expression apply(Expression expr) {
                if (expr instanceof UnresolvedAttribute) {
                    // Add the name of the UnresolvedAttribute to the list
                    columnNames.add(((UnresolvedAttribute) expr).name());
                }
                return expr;
            }

            @Override
            public boolean isDefinedAt(Expression expr) {
                return expr instanceof UnresolvedAttribute;
            }
        });

        return columnNames;

    }


    public static Map<String, List<String>> createReverseColumnDependencyMap(Map<String, List<String>> columnDependencies) {
        Map<String, List<String>> reverseDependencies = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : columnDependencies.entrySet()) {
            String query = entry.getKey();
            List<String> columns = entry.getValue();
            for (String column : columns) {
                if (!reverseDependencies.containsKey(column)) {
                    reverseDependencies.put(column, new ArrayList<>());
                }
                reverseDependencies.get(column).add(query);
            }
        }
        return reverseDependencies;
    }

    public static List<String> getColumnImpact(String column, Map<String, List<String>> reverseColumnDependencies) {
        return reverseColumnDependencies.getOrDefault(column, Collections.emptyList());
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
