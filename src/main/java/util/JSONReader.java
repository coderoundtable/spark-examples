package util;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class JSONReader {

    private Map<String, Integer> queries;  // Map of view name to SQL query
    private Map<String, List<String>> dependencies;  // Map of view name to a list of dependencies
    private List<String> writeToTable;  // List of views to write to tables

    //create a json read method to read the json file and return the queries, dependencies and writeToTable
    public JSONReader(String jsonFilePath) throws IOException {
        // Read the JSON file
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> jsonMap = objectMapper.readValue(new File(jsonFilePath), Map.class);

        // Extract the queries, dependencies, and writeToTable
        this.queries = (Map<String, Integer>) jsonMap.get("queries");
        this.dependencies = (Map<String, List<String>>) jsonMap.get("dependencies");
        this.writeToTable = (List<String>) jsonMap.get("writeToTable");
    }

    //create getter for queries
    public Map<String, Integer> getQueries() {
        return queries;
    }

    //create getter for dependencies
    public Map<String, List<String>> getDependencies() {
        return dependencies;
    }

    //create getter for writeToTable
    public List<String> getWriteToTable() {
        return writeToTable;
    }


}
