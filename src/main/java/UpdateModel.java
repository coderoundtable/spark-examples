import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import net.sf.jsqlparser.util.deparser.StatementDeParser;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UpdateModel {
    public static void main(String[] args) throws Exception {
        // Load column mappings from CSV
        Map<String, Map<String, String>> columnMappings = loadColumnMappings("src/main/resources/column_mappings.csv");

        // Parse SQL files and replace column names
        for (String queryName : columnMappings.keySet()) {
            String sql = new String(Files.readAllBytes(Paths.get("src/main/resources/sqls/" + queryName + ".sql")));
            Statement statement = CCJSqlParserUtil.parse(sql);

            // Create a new StatementDeParser that will replace column names
            ExpressionDeParser expressionDeParser = new ExpressionDeParser() {
                @Override
                public void visit(Column column) {
                    Table table = column.getTable();
                    if (table != null) {
                        String tableName = table.getName();
                        String columnName = column.getColumnName();
                        Map<String, String> tableMappings = columnMappings.get(tableName);
                        if (tableMappings != null && tableMappings.containsKey(columnName)) {
                            column.setColumnName(tableMappings.get(columnName));
                        }
                    }
                    super.visit(column);
                }
            };
            StringBuilder buffer = new StringBuilder();
            SelectDeParser selectDeParser = new SelectDeParser(expressionDeParser, buffer);
            expressionDeParser.setSelectVisitor(selectDeParser);
            expressionDeParser.setBuffer(buffer);
            StatementDeParser deParser = new StatementDeParser(expressionDeParser, selectDeParser, buffer);

            // Deparse the statement to apply the column name replacements
            statement.accept(deParser);

            // Write the modified SQL to a new directory
            Files.write(Paths.get("src/main/resources/updated_sqls/" + queryName + ".sql"), buffer.toString().getBytes());
        }
    }

    private static Map<String, Map<String, String>> loadColumnMappings(String csvFile) throws IOException, CsvException {
        Map<String, Map<String, String>> columnMappings = new HashMap<>();
        try (CSVReader reader = new CSVReader(new FileReader(csvFile))) {
            List<String[]> lines = reader.readAll();
            for (String[] line : lines) {
                String tableName = line[0];
                String oldColumnName = line[1];
                String newColumnName = line[2];
                columnMappings.computeIfAbsent(tableName, k -> new HashMap<>()).put(oldColumnName, newColumnName);
            }
        }
        return columnMappings;
    }
}
