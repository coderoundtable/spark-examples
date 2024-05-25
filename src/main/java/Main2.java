import net.sf.jsqlparser.JSQLParserException;

import java.io.IOException;
import java.util.*;

public class Main2 {
    // Map to store dependencies
    static Map<String, List<String>> dependencies = new HashMap<>();

    public static void main(String[] args) throws IOException, JSQLParserException {
        // Assume dependencies are populated
        Map<String, List<String>> dependencies =  Main1.retrievDependencies();
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
