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
        
        bigQueryUtil.insertData(datasetId, tableId, avroRecords);
        
        Thread.sleep(5000);
    }
    
    @Then("I should retrieve the same data from BigQuery")
    public void i_should_retrieve_the_same_data_from_bigquery() throws Exception {
        retrievedData = bigQueryUtil.queryData(datasetId, tableId, timestampSuffix);
        
        System.out.println("DEBUG: Retrieved data from BigQuery:");
        for (int i = 0; i < retrievedData.size(); i++) {
            System.out.println("DEBUG: Retrieved data [" + i + "]: " + retrievedData.get(i));
        }
    }
    
    @Then("the retrieved data should match the original Cucumber data")
    public void the_retrieved_data_should_match_the_original_cucumber_data() {
        assertEquals("Data count should match", cucumberData.size(), retrievedData.size());
        
        for (int i = 0; i < cucumberData.size(); i++) {
            Map<String, Object> original = cucumberData.get(i);
            Map<String, Object> retrieved = retrievedData.get(i);
            
            assertEquals("Order ID should match", original.get("orderId"), retrieved.get("orderId"));
            
            // Validate nested customer data
            Map<String, Object> originalCustomer = (Map<String, Object>) original.get("customer");
            Map<String, Object> retrievedCustomer = (Map<String, Object>) retrieved.get("customer");
            assertEquals("Customer ID should match", originalCustomer.get("customerId"), retrievedCustomer.get("customerId"));
            assertEquals("Customer name should match", originalCustomer.get("name"), retrievedCustomer.get("name"));
            assertEquals("Customer email should match", originalCustomer.get("email"), retrievedCustomer.get("email"));
            
            assertEquals("Order date should match", original.get("orderDate"), retrieved.get("orderDate"));
            assertEquals("Payment method should match", original.get("paymentMethod"), retrieved.get("paymentMethod"));
            assertEquals("Status should match", original.get("status"), retrieved.get("status"));
            assertEquals("Metadata should match", original.get("metadata"), retrieved.get("metadata"));
            assertEquals("Total amount should match", original.get("totalAmount"), retrieved.get("totalAmount"));
            assertEquals("Tax amount should match", original.get("taxAmount"), retrieved.get("taxAmount"));
            assertEquals("Discount applied should match", original.get("discountApplied"), retrieved.get("discountApplied"));
        }
    }
    
    @After
    public void cleanup() {
        // No cleanup needed - table should remain
    }
}