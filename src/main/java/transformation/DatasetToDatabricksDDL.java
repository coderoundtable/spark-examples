package transformation;

import org.apache.spark.sql.Dataset;
        import org.apache.spark.sql.Row;
        import org.apache.spark.sql.SparkSession;
        import org.apache.spark.sql.types.*;

public class DatasetToDatabricksDDL {

    public static void main(String[] args) {
        // Initialize Spark session
        SparkSession spark = SparkSession.builder()
                .appName("ParquetToDatabricksDDL")
                .master("local[*]")
                .getOrCreate();

        // Path to your Parquet file
        String parquetFilePath = "path/to/your/parquet/file";

        // Load Parquet file
        Dataset<Row> df = spark.read().parquet(parquetFilePath);

        // Get schema
        StructType schema = df.schema();

        // Generate CREATE TABLE statement
        String createTableStatement = generateCreateTableStatement(schema, "table_name");
        System.out.println(createTableStatement);

        // Stop Spark session
        spark.stop();
    }

    private static String generateCreateTableStatement(StructType schema, String tableName) {
        StringBuilder createTableStatement = new StringBuilder("CREATE TABLE ").append(tableName).append(" (\n");

        for (StructField field : schema.fields()) {
            String columnName = field.name();
            String dataType = mapType(field.dataType());

            createTableStatement.append("  ").append(columnName).append(" ").append(dataType).append(",\n");
        }
        createTableStatement.setLength(createTableStatement.length() - 2);
        createTableStatement.append("\n);");

        return createTableStatement.toString();
    }

    private static String mapType(DataType dataType) {
        if (dataType instanceof IntegerType) {
            return "INT";
        } else if (dataType instanceof LongType) {
            return "BIGINT";
        } else if (dataType instanceof BooleanType) {
            return "BOOLEAN";
        } else if (dataType instanceof FloatType) {
            return "FLOAT";
        } else if (dataType instanceof DoubleType) {
            return "DOUBLE";
        } else if (dataType instanceof StringType) {
            return "STRING";
        } else if (dataType instanceof BinaryType) {
            return "BINARY";
        } else if (dataType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) dataType;
            return "DECIMAL(" + decimalType.precision() + ", " + decimalType.scale() + ")";
        } else if (dataType instanceof TimestampType) {
            return "TIMESTAMP";
        } else if (dataType instanceof DateType) {
            return "DATE";
        } else {
            return "STRING";
        }
    }
}
