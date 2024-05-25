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

public class Main2 {
    // Map to store dependencies
    static Map<String, List<String>> dependencies = new HashMap<>();

    public static void main(String[] args) throws IOException, JSQLParserException {
        // Assume dependencies are populated
        Map<String, List<String>> dependencies =  retrieveDependencies ();
        String viewName = "aggregate_view"; // replace with your view name
        Node root = new Node(viewName);
        buildDependencyTree(root, dependencies);

        System.out.println(root.prettyPrint());
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

    public static Map<String, List<String>> retrieveDependencies() throws IOException, JSQLParserException {
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
