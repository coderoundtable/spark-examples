import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.plans.logical.*;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import runners.QueryRunner;
import scala.collection.JavaConverters;
import scala.runtime.AbstractPartialFunction;
import util.JSONReader;
import util.SparkUtil;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class ExtractLineage {

    private static final Map<String, Object> lineageMap = new HashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String keyName = "Lineage";

    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession.builder()
                .appName("Lineage Extractor")
                .master("local")
                .getOrCreate();

        ExtractLineage lineageExtractor = new ExtractLineage();

        // Load JSON file
        String jsonPath = "src/main/resources/dependencies.json";
        String sqlFilesDirectory = "src/main/resources/sqls";

        String tableName = "Reportingtable";
        JSONReader jsonReader = new JSONReader(jsonPath);

        //load all csv files and create views
        SparkUtil.loadCSV(spark, "src/main/resources/customers-100.csv", "customers");

        // Create a QueryRunner object
        QueryRunner queryRunner = new QueryRunner(spark, jsonReader, sqlFilesDirectory);

        // Run all queries
        queryRunner.runAllQueries();

        // Write specific views to tables (if needed)

        List<String> viewsToWrite = jsonReader.getWriteToTable();
        Dataset<Row> finalReportDf = SparkUtil.getDatasetFromView(spark, viewsToWrite.get(0));

        // Example DataFrame loading tables and creating views
        // ... (same as before)

        lineageExtractor.captureLineage(finalReportDf, "Reporting table");

//        // Print the captured lineage information in JSON format
//        lineageExtractor.printLineageAsJson();

        lineageExtractor.writeLineageToFile("data-dependencies.json");

        spark.stop();
    }

    private void captureLineage(Dataset<Row> dataset, String tableName) {
        LogicalPlan logicalPlan = dataset.queryExecution().analyzed();

        // Collect all nodes from the logical plan
        List<LogicalPlan> allNodes = JavaConverters.seqAsJavaList(
                logicalPlan.collect(
                        new AbstractPartialFunction<LogicalPlan, LogicalPlan>() {
                            @Override
                            public boolean isDefinedAt(LogicalPlan x) {
                                return true; // This function is defined for all nodes
                            }

                            @Override
                            public LogicalPlan apply(LogicalPlan v1) {
                                return v1; // Return the node itself
                            }
                        }
                )
        );

        // Recursively find the source DataFrames or views
        DependencyNode dependencies = findDependencies(logicalPlan);

        dependencies.prettyPrint(dependencies);
        // Convert the DependencyNode tree to a set of source table names
//        Set<String> sourceTables = convertToTableNames(dependencies);

        List<TransformationInfo> transformationList = extractTransformations(allNodes);

        lineageMap.put(keyName, new TableLineageInfo(tableName, dependencies, transformationList));

    }

    private void printLineageAsJson() {
        try {
            String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(lineageMap);
            System.out.println(json);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private DependencyNode findDependencies(LogicalPlan plan) {
        if (plan instanceof UnresolvedRelation) {
            // Handle unresolved relations, typically views or tables
            UnresolvedRelation unresolvedRelation = (UnresolvedRelation) plan;
            return new DependencyNode(unresolvedRelation.tableName());
        } else if (plan instanceof SubqueryAlias) {
            // Handle subquery aliases which can represent views
            SubqueryAlias alias = (SubqueryAlias) plan;
            DependencyNode node = new DependencyNode(alias.alias());
            // Recursively find dependencies in the child of the alias
            DependencyNode childNode = findDependencies(alias.child());
            if (childNode != null) {
                node.addChild(childNode);
            }
            return node;
        } else {
            // Recursively process children nodes to find tables or views
            for (LogicalPlan child : JavaConverters.seqAsJavaList(plan.children())) {
                DependencyNode childNode = findDependencies(child);
                if (childNode != null) {
                    // If a child node representing a table or view is found, return it
                    return childNode;
                }
            }
        }
        // Return null if the plan does not represent a table or view
        return null;
    }

    private void writeLineageToFile(String filePath) {
        try {
            // Convert the lineage map to JSON
            String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(lineageMap);

            // Write JSON to the specified file
            try (FileWriter fileWriter = new FileWriter(filePath)) {
                fileWriter.write(json);
            }

            System.out.println("Lineage information has been written to: " + filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


//    private Set<String> findDependencies(LogicalPlan plan) {
//        Set<String> dependencies = new HashSet<>();
//        if (plan instanceof UnresolvedRelation) {
//            // Handle unresolved relations, typically views or tables
//            UnresolvedRelation unresolvedRelation = (UnresolvedRelation) plan;
//            dependencies.add(unresolvedRelation.tableName());
//        } else if (plan instanceof SubqueryAlias) {
//            // Handle subquery aliases which can represent views
//            SubqueryAlias alias = (SubqueryAlias) plan;
//            dependencies.add(alias.alias());
//            // Recursively find dependencies in the child of the alias
//            dependencies.addAll(findDependencies(alias.child()));
//        }
//        // Recursively find dependencies in child nodes
//        for (LogicalPlan child : JavaConverters.seqAsJavaList(plan.children())) {
//            dependencies.addAll(findDependencies(child));
//        }
//        return dependencies;
//    }

    private Set<String> convertToTableNames(DependencyNode node) {
        Set<String> tableNames = new HashSet<>();
        if (node.children.isEmpty()) {
            tableNames.add(node.name);
        } else {
            for (DependencyNode child : node.children) {
                tableNames.addAll(convertToTableNames(child));
            }
        }
        return tableNames;
    }


    private List<TransformationInfo> extractTransformations(List<LogicalPlan> allNodes) {
        List<TransformationInfo> transformations = new ArrayList<>();
        for (LogicalPlan node : allNodes) {
            if (node instanceof Project) {
                Project project = (Project) node;
                transformations.add(new TransformationInfo("Select", "Selected columns: " + project.projectList()));
            } else if (node instanceof Filter) {
                Filter filter = (Filter) node;
                transformations.add(new TransformationInfo("Filter", "Filtered rows where: " + filter.condition()));
            } else if (node instanceof Aggregate) {
                Aggregate aggregate = (Aggregate) node;
                transformations.add(new TransformationInfo("Aggregate", "Aggregated by: " + aggregate.groupingExpressions() + ", with aggregations: " + aggregate.aggregateExpressions()));
            } else if (node instanceof Sort) {
                Sort sort = (Sort) node;
                transformations.add(new TransformationInfo("Sort", "Sorted by: " + sort.order()));
            }
            // Add more cases for other types of transformations as needed
        }
        return transformations;
    }

        class TransformationInfo {
            private final String transformationType;
            private final String details;

            public TransformationInfo(String transformationType, String details) {
                this.transformationType = transformationType;
                this.details = details;
            }

            public String getTransformationType() {
                return transformationType;
            }

            public String getDetails() {
                return details;
            }
        }

    private static class TableLineageInfo {
        private final String tableName;
        private final DependencyNode dependencyTree; // Changed from sourceTableNames to dependencyTree
        private final List<TransformationInfo> transformations;

        public TableLineageInfo(String tableName, DependencyNode dependencyTree, List<TransformationInfo> transformations) {
            this.tableName = tableName;
            this.dependencyTree = dependencyTree;
            this.transformations = transformations;
        }

        // Getters

        public String getTableName() {
            return tableName;
        }

        public DependencyNode getDependencyTree() {
            return dependencyTree;
        }

        public List<TransformationInfo> getTransformations() {
            return transformations;
        }
    }

    private static class DependencyNode {
        String name;
        List<DependencyNode> children = new ArrayList<>();

        DependencyNode(String name) {
            this.name = name;
        }

        public void addChild(DependencyNode child) {
            this.children.add(child);
        }

        public String getName() {
            return name;
        }

        public List<DependencyNode> getChildren() {
            return children;
        }

        void prettyPrint(DependencyNode dependencyNode) {
             prettyPrintDependencies(dependencyNode, "");
        }

        public void prettyPrintDependencies(DependencyNode node, String indent) {
            if (node == null) {
                return;
            }
            // Print the current node with the indent
            System.out.println(indent + "- " + node.getName());
            // Recursively print children with an increased indent
            for (DependencyNode child : node.getChildren()) {
                prettyPrintDependencies(child, indent + "  ");
            }
        }
    }


}
