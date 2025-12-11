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
            Map<String, Object> originalData = cucumberData.get(i);
            String retrievedJson = jsonData.get(i);
            
            System.out.println("DEBUG: Comparing record " + i);
            System.out.println("DEBUG: Original data: " + JsonUtil.toJson(originalData));
            System.out.println("DEBUG: Retrieved JSON: " + retrievedJson);
            
            JsonNode originalJson = JsonUtil.parseJson(JsonUtil.toJson(originalData));
            JsonNode retrievedJsonNode = JsonUtil.parseJson(retrievedJson);
            
            // Compare key fields
            assertTrue("Order ID should match", 
                JsonUtil.compareJsonValues(originalJson.get("orderId"), retrievedJsonNode.get("order_id"), "orderId"));
            
            // Compare customer data with field name mapping
            JsonNode originalCustomer = originalJson.get("customer");
            JsonNode retrievedCustomer = retrievedJsonNode.get("customer");
            
            if (originalCustomer != null && retrievedCustomer != null) {
                assertTrue("Customer ID should match", 
                    JsonUtil.compareJsonValues(originalCustomer.get("customerId"), retrievedCustomer.get("customer_id"), "customer.customerId"));
                assertTrue("Customer name should match", 
                    JsonUtil.compareJsonValues(originalCustomer.get("name"), retrievedCustomer.get("name"), "customer.name"));
                assertTrue("Customer email should match", 
                    JsonUtil.compareJsonValues(originalCustomer.get("email"), retrievedCustomer.get("email"), "customer.email"));
                assertTrue("Customer phone should match", 
                    JsonUtil.compareJsonValues(originalCustomer.get("phone"), retrievedCustomer.get("phone"), "customer.phone"));
                assertTrue("Customer loyalty tier should match", 
                    JsonUtil.compareJsonValues(originalCustomer.get("loyaltyTier"), retrievedCustomer.get("loyalty_tier"), "customer.loyaltyTier"));
            }
            
            // Compare items array
            JsonNode originalItems = originalJson.get("items");
            JsonNode retrievedItems = retrievedJsonNode.get("items");
            
            if (originalItems != null && retrievedItems != null && originalItems.isArray() && retrievedItems.isArray()) {
                assertEquals("Items array size should match", originalItems.size(), retrievedItems.size());
                for (int j = 0; j < originalItems.size(); j++) {
                    JsonNode originalItem = originalItems.get(j);
                    JsonNode retrievedItem = retrievedItems.get(j);
                    
                    assertTrue("Item product ID should match", 
                        JsonUtil.compareJsonValues(originalItem.get("productId"), retrievedItem.get("product_id"), "items[" + j + "].productId"));
                    assertTrue("Item product name should match", 
                        JsonUtil.compareJsonValues(originalItem.get("productName"), retrievedItem.get("product_name"), "items[" + j + "].productName"));
                    assertTrue("Item quantity should match", 
                        JsonUtil.compareJsonValues(originalItem.get("quantity"), retrievedItem.get("quantity"), "items[" + j + "].quantity"));
                    assertTrue("Item unit price should match", 
                        JsonUtil.compareJsonValues(originalItem.get("unitPrice"), retrievedItem.get("unit_price"), "items[" + j + "].unitPrice"));
                    assertTrue("Item category should match", 
                        JsonUtil.compareJsonValues(originalItem.get("category"), retrievedItem.get("category"), "items[" + j + "].category"));
                }
            }
            
            // Compare shipping address
            JsonNode originalAddress = originalJson.get("shippingAddress");
            JsonNode retrievedAddress = retrievedJsonNode.get("shipping_address");
            
            if (originalAddress != null && retrievedAddress != null) {
                assertTrue("Address street should match", 
                    JsonUtil.compareJsonValues(originalAddress.get("street"), retrievedAddress.get("street"), "shippingAddress.street"));
                assertTrue("Address city should match", 
                    JsonUtil.compareJsonValues(originalAddress.get("city"), retrievedAddress.get("city"), "shippingAddress.city"));
                assertTrue("Address state should match", 
                    JsonUtil.compareJsonValues(originalAddress.get("state"), retrievedAddress.get("state"), "shippingAddress.state"));
                assertTrue("Address zip code should match", 
                    JsonUtil.compareJsonValues(originalAddress.get("zipCode"), retrievedAddress.get("zip_code"), "shippingAddress.zipCode"));
                assertTrue("Address country should match", 
                    JsonUtil.compareJsonValues(originalAddress.get("country"), retrievedAddress.get("country"), "shippingAddress.country"));
            }
            
            // Compare other order fields
            assertTrue("Order date should match", 
                JsonUtil.compareJsonValues(originalJson.get("orderDate"), retrievedJsonNode.get("order_date"), "orderDate"));
            assertTrue("Payment method should match", 
                JsonUtil.compareJsonValues(originalJson.get("paymentMethod"), retrievedJsonNode.get("payment_method"), "paymentMethod"));
            assertTrue("Status should match", 
                JsonUtil.compareJsonValues(originalJson.get("status"), retrievedJsonNode.get("status"), "status"));
            assertTrue("Metadata should match", 
                JsonUtil.compareJsonValues(originalJson.get("metadata"), retrievedJsonNode.get("metadata"), "metadata"));
            assertTrue("Total amount should match", 
                JsonUtil.compareJsonValues(originalJson.get("totalAmount"), retrievedJsonNode.get("total_amount"), "totalAmount"));
            assertTrue("Tax amount should match", 
                JsonUtil.compareJsonValues(originalJson.get("taxAmount"), retrievedJsonNode.get("tax_amount"), "taxAmount"));
            assertTrue("Discount applied should match", 
                JsonUtil.compareJsonValues(originalJson.get("discountApplied"), retrievedJsonNode.get("discount_applied"), "discountApplied"));
            
            System.out.println("DEBUG: Record " + i + " validation passed");
        }
    }
    
    @After
    public void cleanup() {
        // No cleanup needed - table should remain
    }
}