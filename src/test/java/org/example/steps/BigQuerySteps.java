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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.example.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Properties;
import java.io.InputStream;

public class BigQuerySteps {
    private List<Map<String, Object>> cucumberData;
    private List<GenericRecord> avroRecords;
    private List<Map<String, Object>> retrievedData;
    private BigQueryUtil bigQueryUtil;
    private Schema schema;
    private String datasetId;
    private String tableId;
    private String timestampSuffix;
    private static final Properties fieldMappings = loadFieldMappings();
    
    public BigQuerySteps() {
        this.bigQueryUtil = new BigQueryUtil();
    }
    
    private static Properties loadFieldMappings() {
        Properties props = new Properties();
        try (InputStream is = BigQuerySteps.class.getResourceAsStream("/field-mappings.properties")) {
            props.load(is);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load field mappings", e);
        }
        return props;
    }
    
    private Map<String, String> getFieldMappings(String prefix) {
        return fieldMappings.stringPropertyNames().stream()
            .filter(key -> key.startsWith(prefix + "."))
            .collect(HashMap::new, (map, key) -> map.put(
                key.substring(prefix.length() + 1), 
                fieldMappings.getProperty(key)
            ), HashMap::putAll);
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
        JsonNode original = JsonUtil.parseJson(JsonUtil.toJson(originalData));
        JsonNode retrieved = JsonUtil.parseJson(retrievedJson);
        
        // Validate root fields
        validateFields(original,
                retrieved,
                getFieldMappings("root"),
                "");
        
        // Validate nested structures
        validateFields(original.get("customer"),
                retrieved.get("customer"),
                getFieldMappings("customer"),
                "Customer ");

        validateFields(original.get("shippingAddress"),
                retrieved.get("shipping_address"),
                getFieldMappings("address"),
                "Address ");
        
        // Validate items array
        if (original.has("items") && retrieved.has("items")) {
            JsonNode origItems = original.get("items");
            JsonNode retrievedItems = retrieved.get("items");
            assertEquals("Items array size should match", origItems.size(), retrievedItems.size());
            
            for (int i = 0; i < origItems.size(); i++) {
                validateFields(origItems.get(i),
                        retrievedItems.get(i),
                        getFieldMappings("item"),
                        "Item[" + i + "] ");
            }
        }
    }
    
    private void validateFields(JsonNode original, JsonNode retrieved, Map<String, String> fieldMappings, String prefix) {
        if (original == null || retrieved == null) return;
        
        fieldMappings.forEach((orig, bq) -> {
            JsonNode expectedVal = original.get(orig);
            JsonNode actualVal = retrieved.get(bq);
            boolean matches = JsonUtil.compareJsonValues(expectedVal, actualVal, orig);
            String arrow = orig.equals(bq) ? "" : " -> " + bq;
            System.out.println(prefix + orig + arrow + ": " + (matches ? "PASS" : "FAIL") + 
                " | Expected: " + expectedVal + " | Actual: " + actualVal);
            assertTrue(prefix + orig + " should match", matches);
        });
    }

    @After
    public void cleanup() {
        // No cleanup needed - table should remain
    }
}