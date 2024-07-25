import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.*;
import runners.QueryRunner;
import scala.collection.JavaConverters;
import scala.runtime.AbstractPartialFunction;
import util.JSONReader;
import util.SparkUtil;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class ExtractLineageV1 {

    private static final Map<String, Object> lineageMap = new HashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String keyName = "Lineage";

    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession.builder()
                .appName("Lineage Extractor")
                .master("local")
                .getOrCreate();

        ExtractLineageV1 lineageExtractor = new ExtractLineageV1();

        // Load JSON file
        String jsonPath = "src/main/resources/dependencies.json";
        String sqlFilesDirectory = "src/main/resources/sqls";

        String tableName = "Reporting table";
        JSONReader jsonReader = new JSONReader(jsonPath);

        // Load all CSV files and create views
        SparkUtil.loadCSV(spark, "src/main/resources/customers-100.csv", "customers");

        // Create a QueryRunner object
        QueryRunner queryRunner = new QueryRunner(spark, jsonReader, sqlFilesDirectory);

        // Run all queries
        queryRunner.runAllQueries();

        // Write specific views to tables (if needed)
        List<String> viewsToWrite = jsonReader.getWriteToTable();
        Dataset<Row> finalReportDf = SparkUtil.getDatasetFromView(spark, viewsToWrite.get(0));

        lineageExtractor.captureLineage(finalReportDf, tableName);
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
        DependencyNode dependencies = null;
        try {
            dependencies = findDependencies(logicalPlan);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        dependencies.prettyPrint(dependencies);
        List<TransformationInfo> transformationList = extractTransformations(allNodes);

        lineageMap.put(keyName, new TableLineageInfo(tableName, dependencies, transformationList));
    }

    private DependencyNode findDependencies(LogicalPlan plan) throws ParseException {
        if (plan instanceof UnresolvedRelation) {
            UnresolvedRelation unresolvedRelation = (UnresolvedRelation) plan;
            DependencyNode node = new DependencyNode(unresolvedRelation.tableName());
            node.addColumns(getColumnNames(plan));
            return node;
        } else if (plan instanceof SubqueryAlias) {
            SubqueryAlias alias = (SubqueryAlias) plan;
            DependencyNode node = new DependencyNode(alias.alias());
            node.addColumns(getColumnNames(plan));
            DependencyNode childNode = findDependencies(alias.child());
            if (childNode != null) {
                node.addChild(childNode);
                node.addColumns(childNode.getColumns());
            }
            return node;
        } else {
            for (LogicalPlan child : JavaConverters.seqAsJavaList(plan.children())) {
                DependencyNode childNode = findDependencies(child);
                if (childNode != null) {
                    return childNode;
                }
            }
        }
        return null;
    }

    public static List<String> getColumnNames( LogicalPlan plan) throws ParseException {
        // Parse the SQL query into a LogicalPlan
//        LogicalPlan plan = spark.sessionState().sqlParser().parsePlan(sql);

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

    private List<String> getColumnsFromRelation(UnresolvedRelation relation) {
        SparkSession spark = SparkSession.builder().getOrCreate();
        Dataset<Row> table = spark.table(relation.tableName());
        return Arrays.asList(table.columns());
    }

    private void writeLineageToFile(String filePath) {
        try {
            String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(lineageMap);
            try (FileWriter fileWriter = new FileWriter(filePath)) {
                fileWriter.write(json);
            }
            System.out.println("Lineage information has been written to: " + filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<TransformationInfo> extractTransformations(List<LogicalPlan> allNodes) {
        List<TransformationInfo> transformations = new ArrayList<>();
        for (LogicalPlan node : allNodes) {
            if (node instanceof Project) {
                Project project = (Project) node;
                List<String> columns = JavaConverters.seqAsJavaList(project.projectList()).stream().map(Object::toString).collect(Collectors.toList());
                transformations.add(new TransformationInfo("Select", "Selected columns: " + columns, columns));
            } else if (node instanceof Filter) {
                Filter filter = (Filter) node;
                List<String> columns = Arrays.asList(filter.condition().toString());
                transformations.add(new TransformationInfo("Filter", "Filtered rows where: " + filter.condition(), columns));
            } else if (node instanceof Aggregate) {
                Aggregate aggregate = (Aggregate) node;
                List<String> columns = JavaConverters.seqAsJavaList(aggregate.groupingExpressions()).stream().map(Object::toString).collect(Collectors.toList());
                transformations.add(new TransformationInfo("Aggregate", "Aggregated by: " + aggregate.groupingExpressions() + ", with aggregations: " + aggregate.aggregateExpressions(), columns));
            } else if (node instanceof Sort) {
                Sort sort = (Sort) node;
                List<String> columns = JavaConverters.seqAsJavaList(sort.order()).stream().map(Object::toString).collect(Collectors.toList());
                transformations.add(new TransformationInfo("Sort", "Sorted by: " + columns, columns));
            }
        }
        return transformations;
    }

    class TransformationInfo {
        private final String transformationType;
        private final String details;
        private final List<String> involvedColumns;

        public TransformationInfo(String transformationType, String details, List<String> involvedColumns) {
            this.transformationType = transformationType;
            this.details = details;
            this.involvedColumns = involvedColumns;
        }

        public String getTransformationType() {
            return transformationType;
        }

        public String getDetails() {
            return details;
        }

        public List<String> getInvolvedColumns() {
            return involvedColumns;
        }
    }

    private static class TableLineageInfo {
        private final String tableName;
        private final DependencyNode dependencyTree;
        private final List<TransformationInfo> transformations;

        public TableLineageInfo(String tableName, DependencyNode dependencyTree, List<TransformationInfo> transformations) {
            this.tableName = tableName;
            this.dependencyTree = dependencyTree;
            this.transformations = transformations;
        }

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
        List<String> columns = new ArrayList<>();
        List<DependencyNode> children = new ArrayList<>();

        DependencyNode(String name) {
            this.name = name;
        }

        public void addChild(DependencyNode child) {
            this.children.add(child);
        }

        public void addColumns(List<String> columns) {
            this.columns.addAll(columns);
        }

        public String getName() {
            return name;
        }

        public List<String> getColumns() {
            return columns;
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
            System.out.println(indent + "- " + node.getName() + " Columns: " + node.getColumns());
            for (DependencyNode child : node.getChildren()) {
                prettyPrintDependencies(child, indent + "  ");
            }
        }
    }
}
