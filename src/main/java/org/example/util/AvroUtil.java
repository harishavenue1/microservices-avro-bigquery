package org.example.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AvroUtil {
    
    public static Schema loadSchema(String schemaPath) throws IOException {
        try (InputStream inputStream = AvroUtil.class.getClassLoader().getResourceAsStream(schemaPath)) {
            return new Schema.Parser().parse(inputStream);
        }
    }
    
    public static List<GenericRecord> createAvroRequestFromData(List<Map<String, Object>> cucumberData, Schema schema) {
        List<GenericRecord> records = new ArrayList<>();
        
        for (Map<String, Object> data : cucumberData) {
            GenericRecord record = new GenericData.Record(schema);
            
            record.put("orderId", data.get("orderId"));
            
            // Create customer nested record
            Map<String, Object> customerData = (Map<String, Object>) data.get("customer");
            System.out.println("DEBUG: Looking for customer field in schema");
            Schema.Field customerField = schema.getField("customer");
            if (customerField == null) {
                System.out.println("DEBUG: Customer field is null!");
                throw new RuntimeException("Customer field not found in schema");
            }
            Schema customerSchema = customerField.schema();
            GenericRecord customerRecord = new GenericData.Record(customerSchema);
            customerRecord.put("customerId", customerData.get("customerId"));
            customerRecord.put("name", customerData.get("name"));
            customerRecord.put("email", customerData.get("email"));
            customerRecord.put("phone", customerData.get("phone"));
            customerRecord.put("loyaltyTier", customerData.get("loyaltyTier"));
            record.put("customer", customerRecord);
            
            // Create items array
            List<Map<String, Object>> itemsData = (List<Map<String, Object>>) data.get("items");
            Schema itemsSchema = schema.getField("items").schema();
            Schema itemSchema = itemsSchema.getElementType();
            List<GenericRecord> itemRecords = new ArrayList<>();
            for (Map<String, Object> itemData : itemsData) {
                GenericRecord itemRecord = new GenericData.Record(itemSchema);
                itemRecord.put("productId", itemData.get("productId"));
                itemRecord.put("productName", itemData.get("productName"));
                itemRecord.put("quantity", itemData.get("quantity"));
                itemRecord.put("unitPrice", itemData.get("unitPrice"));
                itemRecord.put("category", itemData.get("category"));
                itemRecords.add(itemRecord);
            }
            record.put("items", itemRecords);
            
            record.put("orderDate", data.get("orderDate"));
            
            // Create shipping address nested record
            Map<String, Object> addressData = (Map<String, Object>) data.get("shippingAddress");
            Schema addressSchema = schema.getField("shippingAddress").schema();
            GenericRecord addressRecord = new GenericData.Record(addressSchema);
            addressRecord.put("street", addressData.get("street"));
            addressRecord.put("city", addressData.get("city"));
            addressRecord.put("state", addressData.get("state"));
            addressRecord.put("zipCode", addressData.get("zipCode"));
            addressRecord.put("country", addressData.get("country"));
            record.put("shippingAddress", addressRecord);
            
            record.put("paymentMethod", data.get("paymentMethod"));
            record.put("status", data.get("status"));
            record.put("metadata", data.get("metadata"));
            record.put("totalAmount", data.get("totalAmount"));
            record.put("taxAmount", data.get("taxAmount"));
            record.put("discountApplied", data.get("discountApplied"));
            
            records.add(record);
        }
        
        return records;
    }
}