import com.google.gson.Gson;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Main1 {
    // Map to store dependencies
    static Map<String, List<String>> dependencies = new HashMap<>();

    public static void main(String[] args) throws IOException, JSQLParserException {
        // Assume dependencies are populated


        //return dependencies from the method
        Map<String, List<String>> dependencies =  retrievDependencies();

        // Analyze full dependencies for a specific view
        String viewName = "aggregate_view"; // replace with your view name
        findDependencies(viewName, 0, dependencies);
    }

    public static Map<String, List<String>> retrievDependencies() throws IOException, JSQLParserException {
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
        return dependencies;
    }

    public static void findDependencies(String viewName, int level, Map<String, List<String>> dependencies) {
        List<String> directDependencies = dependencies.get(viewName);
        if (directDependencies != null) {
            for (String dependency : directDependencies) {
                for (int i = 0; i < level; i++) {
                    System.out.print("\t");
                }
                System.out.println(level + " : " + viewName + " : " + dependency);
                findDependencies(dependency, level + 1, dependencies);
            }
        }
    }
}
