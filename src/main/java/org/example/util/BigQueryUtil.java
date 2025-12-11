package org.example.util;

import com.google.cloud.bigquery.*;
import org.apache.avro.generic.GenericRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BigQueryUtil {
    private final BigQuery bigQuery;
    
    public BigQueryUtil() {
        this.bigQuery = BigQueryOptions.newBuilder()
            .setProjectId("kafka-microservice-bigquery")
            .build()
            .getService();
    }

    public void insertDataAsJson(String datasetId, String tableId, List<GenericRecord> records) throws InterruptedException {
        String projectId = "kafka-microservice-bigquery";
        TableId table = TableId.of(projectId, datasetId, tableId);
        
        System.out.println("DEBUG: Using JSON streaming insert for table: " + table);
        
        List<InsertAllRequest.RowToInsert> rows = new ArrayList<>();
        for (int i = 0; i < records.size(); i++) {
            GenericRecord record = records.get(i);
            
            // Convert Avro record to JSON string
            String jsonString = record.toString();
            System.out.println("DEBUG: Avro JSON [" + i + "]: " + jsonString);
            
            // Parse JSON and convert field names to BigQuery format
            try {
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                @SuppressWarnings("unchecked")
                Map<String, Object> jsonMap = mapper.readValue(jsonString, Map.class);
                
                // Convert field names from camelCase to underscore_separated
                Map<String, Object> rowContent = convertFieldNames(jsonMap);
                
                System.out.println("DEBUG: BigQuery JSON insert row [" + i + "]: " + rowContent);
                rows.add(InsertAllRequest.RowToInsert.of(rowContent));
            } catch (Exception e) {
                System.out.println("DEBUG: Error converting JSON: " + e.getMessage());
                throw new RuntimeException("Failed to convert Avro to JSON", e);
            }
        }
        
        InsertAllRequest insertRequest = InsertAllRequest.newBuilder(table)
            .setRows(rows)
            .build();
            
        System.out.println("DEBUG: Sending JSON insert request to BigQuery...");
        InsertAllResponse response = bigQuery.insertAll(insertRequest);
        
        System.out.println("DEBUG: BigQuery JSON insert response received");
        if (response.hasErrors()) {
            System.out.println("DEBUG: Insert errors: " + response.getInsertErrors());
            for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                System.out.println("DEBUG: Row " + entry.getKey() + " errors: " + entry.getValue());
            }
        } else {
            System.out.println("DEBUG: JSON data inserted successfully - no errors");
        }
    }
    
    @SuppressWarnings("unchecked")
    private Map<String, Object> convertFieldNames(Map<String, Object> input) {
        Map<String, Object> output = new HashMap<>();
        
        for (Map.Entry<String, Object> entry : input.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            
            // Convert camelCase to underscore_separated
            String convertedKey = camelToUnderscore(key);
            
            if (value instanceof Map) {
                output.put(convertedKey, convertFieldNames((Map<String, Object>) value));
            } else if (value instanceof List) {
                List<Object> list = (List<Object>) value;
                List<Object> convertedList = new ArrayList<>();
                for (Object item : list) {
                    if (item instanceof Map) {
                        convertedList.add(convertFieldNames((Map<String, Object>) item));
                    } else {
                        convertedList.add(item);
                    }
                }
                output.put(convertedKey, convertedList);
            } else {
                output.put(convertedKey, value);
            }
        }
        return output;
    }
    
    private String camelToUnderscore(String camelCase) {
        return camelCase.replaceAll("([a-z])([A-Z])", "$1_$2").toLowerCase();
    }

    public List<String> queryDataAsJson(String datasetId, String tableId, String timestampSuffix) throws InterruptedException {
        String projectId = "kafka-microservice-bigquery";
        String query = String.format("SELECT TO_JSON_STRING(t) as json_data FROM `%s.%s.%s` t WHERE order_id IN ('ORD_001_%s', 'ORD_002_%s') ORDER BY order_id", projectId, datasetId, tableId, timestampSuffix, timestampSuffix);
        
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
        TableResult result = bigQuery.query(queryConfig);
        
        List<String> jsonData = new ArrayList<>();
        for (FieldValueList row : result.iterateAll()) {
            String json = row.get(0).getStringValue();
            jsonData.add(json);
        }
        return jsonData;
    }
}