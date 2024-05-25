import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.io.FileWriter;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws Exception {
        // Load fromJson from JSON
        String json = new String(Files.readAllBytes(Paths.get("src/main/resources/dependencies.json")));
        Map<String, Object> fromJson = new Gson().fromJson(json, Map.class);

        // Parse SQL files to find dependencies
        TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
        Map<String, List<String>> dependencies = new HashMap<>();
        for (String queryName : ((Map<String, Double>) fromJson.get("queries")).keySet()) {
            String sql = new String(Files.readAllBytes(Paths.get("src/main/resources/sqls/" + queryName + ".sql")));
            Statement statement = CCJSqlParserUtil.parse(new StringReader(sql));
            List<String> tableList = tablesNamesFinder.getTableList(statement);

            // Add dependencies to the map
            dependencies.put(queryName, tableList);
        }

        // Write dependencies to a JSON file
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        try (FileWriter writer = new FileWriter("dependencies_output.json")) {
            gson.toJson(dependencies, writer);
        }
    }
}
