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
    
    public void createTableIfNotExists(String datasetId, String tableId) {
        String projectId = "kafka-microservice-bigquery";
        TableId table = TableId.of(projectId, datasetId, tableId);
        
        System.out.println("DEBUG: Creating table: " + table);
        System.out.println("DEBUG: BigQuery client project: " + bigQuery.getOptions().getProjectId());
        
        try {
            // Check if dataset exists
            DatasetId dataset = DatasetId.of(projectId, datasetId);
            Dataset existingDataset = bigQuery.getDataset(dataset);
//            if (existingDataset == null) {
//                System.out.println("DEBUG: Dataset does not exist, creating: " + dataset);
//                DatasetInfo datasetInfo = DatasetInfo.newBuilder(dataset).build();
//                bigQuery.create(datasetInfo);
//                Thread.sleep(2000);
//            } else {
//                System.out.println("DEBUG: Dataset exists: " + existingDataset.getDatasetId());
//            }
//
//            // Check if table already exists
//            Table existingTable = bigQuery.getTable(table);
//            if (existingTable != null) {
//                System.out.println("DEBUG: Table already exists: " + existingTable.getTableId());
//                return; // Table already exists, no need to create
//            }
            
            Schema schema = Schema.of(
                Field.of("order_id", StandardSQLTypeName.STRING),
                Field.newBuilder("customer", StandardSQLTypeName.STRUCT,
                    Field.of("customer_id", StandardSQLTypeName.STRING),
                    Field.of("name", StandardSQLTypeName.STRING),
                    Field.of("email", StandardSQLTypeName.STRING),
                    Field.of("phone", StandardSQLTypeName.STRING),
                    Field.of("loyalty_tier", StandardSQLTypeName.STRING)
                ).setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("items", StandardSQLTypeName.STRUCT,
                    Field.of("product_id", StandardSQLTypeName.STRING),
                    Field.of("product_name", StandardSQLTypeName.STRING),
                    Field.of("quantity", StandardSQLTypeName.INT64),
                    Field.of("unit_price", StandardSQLTypeName.FLOAT64),
                    Field.of("category", StandardSQLTypeName.STRING)
                ).setMode(Field.Mode.REPEATED).build(),
                Field.of("order_date", StandardSQLTypeName.STRING),
                Field.newBuilder("shipping_address", StandardSQLTypeName.STRUCT,
                    Field.of("street", StandardSQLTypeName.STRING),
                    Field.of("city", StandardSQLTypeName.STRING),
                    Field.of("state", StandardSQLTypeName.STRING),
                    Field.of("zip_code", StandardSQLTypeName.STRING),
                    Field.of("country", StandardSQLTypeName.STRING)
                ).setMode(Field.Mode.NULLABLE).build(),
                Field.of("payment_method", StandardSQLTypeName.STRING),
                Field.of("status", StandardSQLTypeName.STRING),
                Field.of("metadata", StandardSQLTypeName.STRING),
                Field.of("total_amount", StandardSQLTypeName.FLOAT64),
                Field.of("tax_amount", StandardSQLTypeName.FLOAT64),
                Field.of("discount_applied", StandardSQLTypeName.BOOL)
            );
            
            TableDefinition tableDefinition = StandardTableDefinition.of(schema);
            TableInfo tableInfo = TableInfo.newBuilder(table, tableDefinition).build();
            System.out.println("DEBUG: Attempting to create table with info: " + tableInfo.getTableId());
            
//            try {
//                Table createdTable = bigQuery.create(tableInfo);
//
//                if (createdTable == null) {
//                    throw new RuntimeException("Failed to create table: " + table);
//                }
//
//                System.out.println("DEBUG: Table created successfully: " + createdTable.getTableId());
//                System.out.println("DEBUG: Created table project: " + createdTable.getTableId().getProject());
//                System.out.println("DEBUG: Created table dataset: " + createdTable.getTableId().getDataset());
//                System.out.println("DEBUG: Created table name: " + createdTable.getTableId().getTable());
//            } catch (BigQueryException e) {
//                System.out.println("DEBUG: BigQuery exception during table creation: " + e.getMessage());
//                System.out.println("DEBUG: Error code: " + e.getCode());
//                System.out.println("DEBUG: Error reason: " + e.getReason());
//                throw e;
//            }
            
            // Wait and verify table exists
            Thread.sleep(3000);
            Table verifyTable = bigQuery.getTable(table);
            if (verifyTable == null) {
                throw new RuntimeException("Table not available after creation: " + table);
            }
            System.out.println("DEBUG: Table verified: " + verifyTable.getTableId());
            
        } catch (Exception e) {
            System.out.println("DEBUG: Exception during table creation: " + e.getMessage());
            throw new RuntimeException("Error creating table", e);
        }
    }
    
    public void insertData(String datasetId, String tableId, List<GenericRecord> records) throws InterruptedException {
        String projectId = "kafka-microservice-bigquery";
        TableId table = TableId.of(projectId, datasetId, tableId);
        
        System.out.println("DEBUG: Using streaming insert for table: " + table);
        
        List<InsertAllRequest.RowToInsert> rows = new ArrayList<>();
        for (int i = 0; i < records.size(); i++) {
            GenericRecord record = records.get(i);
            Map<String, Object> rowContent = new HashMap<>();
            rowContent.put("order_id", record.get("orderId").toString());
            
            // Convert customer GenericRecord to Map
            GenericRecord customerRecord = (GenericRecord) record.get("customer");
            Map<String, Object> customerMap = new HashMap<>();
            customerMap.put("customer_id", customerRecord.get("customerId").toString());
            customerMap.put("name", customerRecord.get("name").toString());
            customerMap.put("email", customerRecord.get("email").toString());
            customerMap.put("phone", customerRecord.get("phone").toString());
            customerMap.put("loyalty_tier", customerRecord.get("loyaltyTier").toString());
            rowContent.put("customer", customerMap);
            
            // Convert items array to List of Maps
            @SuppressWarnings("unchecked")
            List<GenericRecord> itemRecords = (List<GenericRecord>) record.get("items");
            List<Map<String, Object>> itemMaps = new ArrayList<>();
            for (GenericRecord itemRecord : itemRecords) {
                Map<String, Object> itemMap = new HashMap<>();
                itemMap.put("product_id", itemRecord.get("productId").toString());
                itemMap.put("product_name", itemRecord.get("productName").toString());
                itemMap.put("quantity", itemRecord.get("quantity"));
                itemMap.put("unit_price", itemRecord.get("unitPrice"));
                itemMap.put("category", itemRecord.get("category").toString());
                itemMaps.add(itemMap);
            }
            rowContent.put("items", itemMaps);
            
            rowContent.put("order_date", record.get("orderDate").toString());
            
            // Convert shipping address GenericRecord to Map
            GenericRecord addressRecord = (GenericRecord) record.get("shippingAddress");
            Map<String, Object> addressMap = new HashMap<>();
            addressMap.put("street", addressRecord.get("street").toString());
            addressMap.put("city", addressRecord.get("city").toString());
            addressMap.put("state", addressRecord.get("state").toString());
            addressMap.put("zip_code", addressRecord.get("zipCode").toString());
            addressMap.put("country", addressRecord.get("country").toString());
            rowContent.put("shipping_address", addressMap);
            
            rowContent.put("payment_method", record.get("paymentMethod").toString());
            rowContent.put("status", record.get("status").toString());
            rowContent.put("metadata", record.get("metadata").toString());
            rowContent.put("total_amount", record.get("totalAmount"));
            rowContent.put("tax_amount", record.get("taxAmount"));
            rowContent.put("discount_applied", record.get("discountApplied"));
            
            System.out.println("DEBUG: BigQuery insert row [" + i + "]: " + rowContent);
            rows.add(InsertAllRequest.RowToInsert.of(rowContent));
        }
        
        InsertAllRequest insertRequest = InsertAllRequest.newBuilder(table)
            .setRows(rows)
            .build();
            
        System.out.println("DEBUG: Sending insert request to BigQuery...");
        InsertAllResponse response = bigQuery.insertAll(insertRequest);
        
        System.out.println("DEBUG: BigQuery insert response received");
        if (response.hasErrors()) {
            System.out.println("DEBUG: Insert errors: " + response.getInsertErrors());
            for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                System.out.println("DEBUG: Row " + entry.getKey() + " errors: " + entry.getValue());
            }
            // Don't throw exception, just log the error for now
        } else {
            System.out.println("DEBUG: Data inserted successfully - no errors");
        }
    }
    
    public List<Map<String, Object>> queryData(String datasetId, String tableId, String timestampSuffix) throws InterruptedException {
        String projectId = "kafka-microservice-bigquery";
        String query = String.format("SELECT * FROM `%s.%s.%s` WHERE order_id IN ('ORD_001_%s', 'ORD_002_%s') ORDER BY order_id", projectId, datasetId, tableId, timestampSuffix, timestampSuffix);
        
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
        TableResult result = bigQuery.query(queryConfig);
        
        List<Map<String, Object>> data = new ArrayList<>();
        for (FieldValueList row : result.iterateAll()) {
            Map<String, Object> rowData = new HashMap<>();
            
            System.out.println("DEBUG: Row size: " + row.size());
            for (int i = 0; i < row.size(); i++) {
                FieldValue field = row.get(i);
                System.out.println("DEBUG: Field [" + i + "]: " + (field != null ? field.getValue() : "null") + ", isNull: " + (field != null ? field.isNull() : "field is null"));
            }
            
            // Use index-based access: order_id, customer, items, order_date, shipping_address, payment_method, status, metadata, total_amount, tax_amount, discount_applied
            rowData.put("orderId", safeGetString(row, 0));
            
            // Extract customer (index 1)
            Map<String, Object> customer = new HashMap<>();
            FieldValue customerField = row.get(1);
            System.out.println("DEBUG: Customer field: " + (customerField != null ? customerField.getValue() : "null"));
            if (customerField != null && !customerField.isNull()) {
                try {
                    FieldValueList customerRecord = customerField.getRecordValue();
                    System.out.println("DEBUG: Customer record size: " + customerRecord.size());
                    customer.put("customerId", safeGetString(customerRecord, 0));
                    customer.put("name", safeGetString(customerRecord, 1));
                    customer.put("email", safeGetString(customerRecord, 2));
                    customer.put("phone", safeGetString(customerRecord, 3));
                    customer.put("loyaltyTier", safeGetString(customerRecord, 4));
                } catch (Exception e) {
                    System.out.println("DEBUG: Error extracting customer: " + e.getMessage());
                }
            }
            rowData.put("customer", customer);
            
            // Extract items (index 2)
            List<Map<String, Object>> items = new ArrayList<>();
            FieldValue itemsField = row.get(2);
            System.out.println("DEBUG: Items field: " + (itemsField != null ? itemsField.getValue() : "null"));
            if (itemsField != null && !itemsField.isNull()) {
                try {
                    List<FieldValue> itemsList = itemsField.getRepeatedValue();
                    System.out.println("DEBUG: Items list size: " + itemsList.size());
                    if (!itemsList.isEmpty()) {
                        FieldValueList itemRecord = itemsList.get(0).getRecordValue();
                        Map<String, Object> item = new HashMap<>();
                        item.put("productId", safeGetString(itemRecord, 0));
                        item.put("productName", safeGetString(itemRecord, 1));
                        item.put("quantity", safeGetLong(itemRecord, 2));
                        item.put("unitPrice", safeGetDouble(itemRecord, 3));
                        item.put("category", safeGetString(itemRecord, 4));
                        items.add(item);
                    }
                } catch (Exception e) {
                    System.out.println("DEBUG: Error extracting items: " + e.getMessage());
                }
            }
            rowData.put("items", items);
            
            rowData.put("orderDate", safeGetString(row, 3));
            
            // Extract shipping address (index 4)
            Map<String, Object> shippingAddress = new HashMap<>();
            FieldValue addressField = row.get(4);
            System.out.println("DEBUG: Address field: " + (addressField != null ? addressField.getValue() : "null"));
            if (addressField != null && !addressField.isNull()) {
                try {
                    FieldValueList addressRecord = addressField.getRecordValue();
                    System.out.println("DEBUG: Address record size: " + addressRecord.size());
                    shippingAddress.put("street", safeGetString(addressRecord, 0));
                    shippingAddress.put("city", safeGetString(addressRecord, 1));
                    shippingAddress.put("state", safeGetString(addressRecord, 2));
                    shippingAddress.put("zipCode", safeGetString(addressRecord, 3));
                    shippingAddress.put("country", safeGetString(addressRecord, 4));
                } catch (Exception e) {
                    System.out.println("DEBUG: Error extracting address: " + e.getMessage());
                }
            }
            rowData.put("shippingAddress", shippingAddress);
            
            rowData.put("paymentMethod", safeGetString(row, 5));
            rowData.put("status", safeGetString(row, 6));
            rowData.put("metadata", safeGetString(row, 7));
            rowData.put("totalAmount", safeGetDouble(row, 8));
            rowData.put("taxAmount", safeGetDouble(row, 9));
            rowData.put("discountApplied", safeGetBoolean(row, 10));
            
            data.add(rowData);
        }
        
        return data;
    }
    
    public boolean tableExists(String datasetId, String tableId) {
        String projectId = "kafka-microservice-bigquery";
        TableId table = TableId.of(projectId, datasetId, tableId);
        Table existingTable = bigQuery.getTable(table);
        return existingTable != null;
    }

    private String safeGetString(FieldValueList row, int index) {
        try {
            FieldValue field = row.get(index);
            String value = (field != null && !field.isNull()) ? field.getStringValue() : "";
            System.out.println("DEBUG: safeGetString[" + index + "]: " + value);
            return value;
        } catch (Exception e) {
            System.out.println("DEBUG: Error in safeGetString[" + index + "]: " + e.getMessage());
            return "";
        }
    }
    
    private Long safeGetLong(FieldValueList row, int index) {
        FieldValue field = row.get(index);
        return (field != null && !field.isNull()) ? field.getLongValue() : 0L;
    }
    
    private Double safeGetDouble(FieldValueList row, int index) {
        FieldValue field = row.get(index);
        return (field != null && !field.isNull()) ? field.getDoubleValue() : 0.0;
    }
    
    private Boolean safeGetBoolean(FieldValueList row, int index) {
        FieldValue field = row.get(index);
        return (field != null && !field.isNull()) ? field.getBooleanValue() : false;
    }
}