package org.example.steps;

import io.cucumber.datatable.DataTable;
import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.example.util.AvroUtil;
import org.example.util.BigQueryUtil;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.example.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.InputStream;
import java.util.stream.Collectors;
import java.util.LinkedHashSet;
import java.util.Collections;
import java.util.Enumeration;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class BigQuerySteps {
    private List<Map<String, Object>> cucumberData;
    private List<GenericRecord> avroRecords;
    private List<Map<String, Object>> retrievedData;
    private BigQueryUtil bigQueryUtil;
    private Schema schema;
    private String datasetId;
    private String tableId;
    private String timestampSuffix;

    
    public BigQuerySteps() {
        this.bigQueryUtil = new BigQueryUtil();
    }
    
    private static final Map<String, String> fieldMappings = loadFieldMappings();
    
    private static Map<String, String> loadFieldMappings() {
        Map<String, String> props = new LinkedHashMap<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                Objects.requireNonNull(BigQuerySteps.class.getResourceAsStream("/field-mappings.properties"))))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty() && !line.startsWith("#")) {
                    String[] parts = line.split("=", 2);
                    if (parts.length == 2) {
                        props.put(parts[0], parts[1]);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load field mappings", e);
        }
        return props;
    }
    

    
    @Given("I have complete order data from Cucumber:")
    public void i_have_complete_order_data_from_cucumber(DataTable dataTable) throws Exception {
        timestampSuffix = String.valueOf(System.currentTimeMillis());
        cucumberData = new ArrayList<>();
        List<Map<String, String>> rows = dataTable.asMaps(String.class, String.class);
        
        for (Map<String, String> row : rows) {
            Map<String, Object> orderData = new HashMap<>();
            
            // Order basic info with timestamp suffix
            orderData.put("orderId", row.get("orderId") + "_" + timestampSuffix);
            orderData.put("orderDate", row.get("orderDate"));
            orderData.put("paymentMethod", row.get("paymentMethod"));
            orderData.put("status", row.get("status"));
            orderData.put("metadata", row.get("metadata"));
            orderData.put("totalAmount", Double.parseDouble(row.get("totalAmount")));
            orderData.put("taxAmount", Double.parseDouble(row.get("taxAmount")));
            orderData.put("discountApplied", Boolean.parseBoolean(row.get("discountApplied")));
            
            // Customer nested object
            Map<String, Object> customer = new HashMap<>();
            customer.put("customerId", row.get("customerId"));
            customer.put("name", row.get("customerName"));
            customer.put("email", row.get("customerEmail"));
            customer.put("phone", row.get("customerPhone"));
            customer.put("loyaltyTier", row.get("loyaltyTier"));
            orderData.put("customer", customer);
            
            // Items array with single item
            List<Map<String, Object>> items = new ArrayList<>();
            Map<String, Object> item = new HashMap<>();
            item.put("productId", row.get("productId"));
            item.put("productName", row.get("productName"));
            item.put("quantity", Long.parseLong(row.get("quantity")));
            item.put("unitPrice", Double.parseDouble(row.get("unitPrice")));
            item.put("category", row.get("category"));
            items.add(item);
            orderData.put("items", items);
            
            // Shipping address nested object
            Map<String, Object> shippingAddress = new HashMap<>();
            shippingAddress.put("street", row.get("street"));
            shippingAddress.put("city", row.get("city"));
            shippingAddress.put("state", row.get("state"));
            shippingAddress.put("zipCode", row.get("zipCode"));
            shippingAddress.put("country", row.get("country"));
            orderData.put("shippingAddress", shippingAddress);
            
            cucumberData.add(orderData);
        }
        
        schema = AvroUtil.loadSchema("orders.avro");
        System.out.println("DEBUG: Loaded schema: " + schema);
        System.out.println("DEBUG: Schema fields:");
        for (Schema.Field field : schema.getFields()) {
            System.out.println("DEBUG: Field name: " + field.name() + ", type: " + field.schema().getType());
        }
    }
    
    @When("I create Avro request from schema using the data")
    public void i_create_avro_request_from_schema_using_the_data() throws Exception {
        System.out.println("DEBUG: Creating Avro records from Cucumber data:");
        for (int i = 0; i < cucumberData.size(); i++) {
            System.out.println("DEBUG: Cucumber data [" + i + "]: " + cucumberData.get(i));
        }
        
        avroRecords = AvroUtil.createAvroRequestFromData(cucumberData, schema);
        
        System.out.println("DEBUG: Created Avro records:");
        for (int i = 0; i < avroRecords.size(); i++) {
            System.out.println("DEBUG: Avro record [" + i + "]: " + avroRecords.get(i));
        }
    }
    
    @When("I load the Avro request to BigQuery table {string}")
    public void i_load_the_avro_request_to_bigquery_table(String tableName) throws Exception {
        String[] parts = tableName.split("\\.");
        datasetId = parts[0];
        tableId = parts[1];
        
        bigQueryUtil.insertDataAsJson(datasetId, tableId, avroRecords);
        
        Thread.sleep(5000);
    }
    
    @Then("I should retrieve the same data from BigQuery")
    public void i_should_retrieve_the_same_data_from_bigquery() throws Exception {
        // Get data as JSON for easier validation
        List<String> jsonData = bigQueryUtil.queryDataAsJson(datasetId, tableId, timestampSuffix);
        
        System.out.println("DEBUG: Retrieved JSON data from BigQuery:");
        for (int i = 0; i < jsonData.size(); i++) {
            System.out.println("DEBUG: JSON data [" + i + "]: " + jsonData.get(i));
        }
    }

    @Then("the retrieved JSON data should match the original data structure")
    public void the_retrieved_json_data_should_match_the_original_data_structure() throws Exception {
        List<String> jsonData = bigQueryUtil.queryDataAsJson(datasetId, tableId, timestampSuffix);
        assertEquals("JSON data count should match", cucumberData.size(), jsonData.size());
        
        for (int i = 0; i < cucumberData.size(); i++) {
            validateRecord(cucumberData.get(i), jsonData.get(i));
        }
    }
    
    private void validateRecord(Map<String, Object> originalData, String retrievedJson) throws Exception {
        System.out.println("\n=== Starting Record Validation ===");
        
        // Parse JSON data for validation
        JsonNode original = JsonUtil.parseJson(JsonUtil.toJson(originalData));
        JsonNode retrieved = JsonUtil.parseJson(retrievedJson);
        
        // Validate each field in properties file order
        fieldMappings.keySet().forEach(field -> {
            String config = fieldMappings.get(field);
            System.out.println("\nValidating field: " + field + " with config: " + config);
            
            // Check if field has nested mappings (contains [ or {)
            if (config.contains("[") || config.contains("{")) {
                System.out.println("  -> Nested field detected");
                validateNestedField(original, retrieved, field, config);
            } else {
                System.out.println("  -> Simple field detected");
                // Simple field validation
                validate(original.get(field), retrieved.get(config), field, field, config);
            }
        });
        
        System.out.println("\n=== Record Validation Complete ===\n");
    }
    
    private void validateNestedField(JsonNode original, JsonNode retrieved, String field, String config) {
        // Parse BigQuery field name and mappings
        String bqField, mappingsStr;

        // shippingAddress=shipping_address:{street=street,city=city,state=state,zipCode=zip_code,country=country}
        if (config.contains(":")) {
            String[] parts = config.split(":");
            bqField = parts[0];
            mappingsStr = parts[1];
            System.out.println("    BigQuery field: " + bqField + ", Mappings: " + mappingsStr);
        }
        // items=[productId=product_id,productName=product_name,quantity=quantity,unitPrice=unit_price,category=category]
        else {
            bqField = field;
            mappingsStr = config;
            System.out.println("    Using field name as BigQuery field: " + bqField);
        }
        
        // Extract field mappings from brackets/braces
        String content = mappingsStr.substring(1, mappingsStr.length() - 1);
        System.out.println("    Field mappings: " + content);
        
        // Get nodes for validation
        JsonNode origNode = original.get(field);
        JsonNode retrievedNode = retrieved.get(bqField);
        
        // Null check
        System.out.println("    Null check - Original: " + (origNode != null) + ", Retrieved: " + (retrievedNode != null));
        assertEquals(field + " should both be present or both be null", origNode != null, retrievedNode != null);
        
        if (origNode != null && retrievedNode != null) {
            if (origNode.isArray()) {
                System.out.println("    -> Array type detected, size: " + origNode.size());
                // Validate each mapping for arrays
                for (String mapping : content.split(",")) {
                    String[] kv = mapping.split("=");
                    validateArrayField(origNode, retrievedNode, field, kv);
                }
            } else {
                System.out.println("    -> Object type detected");
                // Validate each mapping for objects
                for (String mapping : content.split(",")) {
                    String[] kv = mapping.split("=");
                    validateObjectField(origNode, retrievedNode, field, kv);
                }
            }
        }
    }
    
    private void validateArrayField(JsonNode origNode, JsonNode retrievedNode, String field, String[] kv) {
        System.out.println("      Array validation - mapping: " + kv[0] + " -> " + kv[1]);
        assertEquals(field + " array size should match", origNode.size(), retrievedNode.size());
        for (int i = 0; i < origNode.size(); i++) {
            String itemName = field.substring(0, field.length() - 1) + "[" + i + "] " + kv[0];
            validate(origNode.get(i).get(kv[0]), retrievedNode.get(i).get(kv[1]), itemName, kv[0], kv[1]);
        }
    }
    
    private void validateObjectField(JsonNode origNode, JsonNode retrievedNode, String field, String[] kv) {
        System.out.println("      Object validation - mapping: " + kv[0] + " -> " + kv[1]);
        String prefix = field.substring(0, 1).toUpperCase() + field.substring(1) + " ";
        validate(origNode.get(kv[0]), retrievedNode.get(kv[1]), prefix + kv[0], kv[0], kv[1]);
    }
    
    private void validate(JsonNode expected, JsonNode actual, String name, String origField, String bqField) {
        boolean matches = JsonUtil.compareJsonValues(expected, actual, origField);
        String arrow = origField.equals(bqField) ? "" : " -> " + bqField;
        System.out.println(name + arrow + ": " + (matches ? "PASS" : "FAIL") + " | Expected: " + expected + " | Actual: " + actual);
        assertTrue(name + " should match", matches);
    }
  
    @After
    public void cleanup() {
        // No cleanup needed - table should remain
    }
}