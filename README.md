# Microservices Avro-BigQuery Project

This project demonstrates JSON to Avro conversion and BigQuery integration with validation using Cucumber tests.

## Setup

1. **Google Cloud Setup**:
   - Create a Google Cloud project
   - Enable BigQuery API
   - Create service account credentials
   - Set `GOOGLE_APPLICATION_CREDENTIALS` environment variable

2. **BigQuery Table Setup**:
   ```sql
   -- Complex BigQuery Schema for Nested Order Structure
   -- Supports nested JSON with STRUCT and ARRAY types
   
   CREATE OR REPLACE TABLE test_dataset.orders (
     order_id STRING NOT NULL,
     
     -- Nested customer object
     customer STRUCT<
       customer_id STRING,
       name STRING,
       email STRING,
       phone STRING,
       loyalty_tier STRING
     >,
     
     -- Array of order items
     items ARRAY<STRUCT<
       product_id STRING,
       product_name STRING,
       quantity INT64,
       unit_price FLOAT64,
       category STRING
     >>,
     
     order_date STRING,
     
     -- Nested shipping address
     shipping_address STRUCT<
       street STRING,
       city STRING,
       state STRING,
       zip_code STRING,
       country STRING
     >,
     
     payment_method STRING,
     status STRING,
     metadata STRING,
     total_amount FLOAT64,
     tax_amount FLOAT64,
     discount_applied BOOL
   )
   OPTIONS(
     description="Complex orders table with nested structures for advanced validation"
   );
   ```

3. **Configuration**:
   - Update `src/main/resources/application.properties` with your project ID

4. **Build**:
   ```bash
   mvn clean compile
   ```

5. **Run Tests**:
   ```bash
   mvn test
   ```

## Project Structure

- `src/main/avro/` - Avro schema files
- `src/main/java/org/example/util/` - Utility classes for Avro and BigQuery
- `src/test/resources/features/` - Cucumber feature files
- `src/test/java/org/example/steps/` - Cucumber step definitions
- `src/test/resources/field-mappings.properties` - Field mapping configuration

## Test Execution Flow

### 1. Data Preparation Phase
```
DEBUG: Loaded schema: {"type":"record","name":"Order","fields":[...]}
DEBUG: Schema fields:
DEBUG: Field name: orderId, type: STRING
DEBUG: Field name: orderDate, type: STRING
DEBUG: Field name: customer, type: RECORD
DEBUG: Field name: items, type: ARRAY
DEBUG: Field name: shippingAddress, type: RECORD
```

### 2. Avro Conversion Phase
```
DEBUG: Creating Avro records from Cucumber data:
DEBUG: Cucumber data [0]: {orderId=ORD001_1703123456789, orderDate=2023-12-01, customer={customerId=CUST001, name=John Doe, email=john@example.com, phone=555-1234, loyaltyTier=Gold}, items=[{productId=PROD001, productName=Laptop, quantity=2, unitPrice=999.99, category=Electronics}], shippingAddress={street=123 Main St, city=New York, state=NY, zipCode=10001, country=USA}, paymentMethod=Credit Card, status=Shipped, metadata=Priority Order, totalAmount=1999.98, taxAmount=159.99, discountApplied=true}

DEBUG: Created Avro records:
DEBUG: Avro record [0]: {"orderId": "ORD001_1703123456789", "orderDate": "2023-12-01", "customer": {"customerId": "CUST001", "name": "John Doe", "email": "john@example.com", "phone": "555-1234", "loyaltyTier": "Gold"}, "items": [{"productId": "PROD001", "productName": "Laptop", "quantity": 2, "unitPrice": 999.99, "category": "Electronics"}], "shippingAddress": {"street": "123 Main St", "city": "New York", "state": "NY", "zipCode": "10001", "country": "USA"}, "paymentMethod": "Credit Card", "status": "Shipped", "metadata": "Priority Order", "totalAmount": 1999.98, "taxAmount": 159.99, "discountApplied": true}
```

### 3. BigQuery Insertion Phase
```
Inserting JSON data to BigQuery table: test_dataset.orders
Data inserted successfully. Rows affected: 1
```

### 4. Data Retrieval Phase
```
DEBUG: Retrieved JSON data from BigQuery:
DEBUG: JSON data [0]: {"order_id":"ORD001_1703123456789","order_date":"2023-12-01","customer":{"customer_id":"CUST001","name":"John Doe","email":"john@example.com","phone":"555-1234","loyalty_tier":"Gold"},"items":[{"product_id":"PROD001","product_name":"Laptop","quantity":2,"unit_price":999.99,"category":"Electronics"}],"shipping_address":{"street":"123 Main St","city":"New York","state":"NY","zip_code":"10001","country":"USA"},"payment_method":"Credit Card","status":"Shipped","metadata":"Priority Order","total_amount":1999.98,"tax_amount":159.99,"discount_applied":true}
```

### 5. Validation Phase
```
orderId -> order_id: PASS | Expected: "ORD001_1703123456789" | Actual: "ORD001_1703123456789"
orderDate -> order_date: PASS | Expected: "2023-12-01" | Actual: "2023-12-01"
paymentMethod -> payment_method: PASS | Expected: "Credit Card" | Actual: "Credit Card"
totalAmount -> total_amount: PASS | Expected: 1999.98 | Actual: 1999.98
taxAmount -> tax_amount: PASS | Expected: 159.99 | Actual: 159.99
discountApplied -> discount_applied: PASS | Expected: true | Actual: true
status: PASS | Expected: "Shipped" | Actual: "Shipped"
metadata: PASS | Expected: "Priority Order" | Actual: "Priority Order"
Customer customerId -> customer_id: PASS | Expected: "CUST001" | Actual: "CUST001"
Customer loyaltyTier -> loyalty_tier: PASS | Expected: "Gold" | Actual: "Gold"
Customer name: PASS | Expected: "John Doe" | Actual: "John Doe"
Customer email: PASS | Expected: "john@example.com" | Actual: "john@example.com"
Customer phone: PASS | Expected: "555-1234" | Actual: "555-1234"
Address zipCode -> zip_code: PASS | Expected: "10001" | Actual: "10001"
Address street: PASS | Expected: "123 Main St" | Actual: "123 Main St"
Address city: PASS | Expected: "New York" | Actual: "New York"
Address state: PASS | Expected: "NY" | Actual: "NY"
Address country: PASS | Expected: "USA" | Actual: "USA"
Item[0] productId -> product_id: PASS | Expected: "PROD001" | Actual: "PROD001"
Item[0] productName -> product_name: PASS | Expected: "Laptop" | Actual: "Laptop"
Item[0] unitPrice -> unit_price: PASS | Expected: 999.99 | Actual: 999.99
Item[0] quantity: PASS | Expected: 2 | Actual: 2
Item[0] category: PASS | Expected: "Electronics" | Actual: "Electronics"
```

## Components Deep Dive

### 1. Avro Schema (`orders.avro`)
**Purpose**: Defines the structure and data types for order records
**Key Features**:
- Nested customer record with personal details
- Array of items with product information
- Shipping address as nested record
- Mixed data types (strings, numbers, booleans)

### 2. AvroUtil Class
**Purpose**: Handles Avro schema loading and GenericRecord creation
**Key Methods**:
- `loadSchema()`: Loads Avro schema from classpath
- `createAvroRequestFromData()`: Converts Map data to GenericRecord
**Logic**: Uses reflection to map JSON-like data to strongly-typed Avro records

### 3. BigQueryUtil Class
**Purpose**: Manages BigQuery operations and data transformation
**Key Methods**:
- `insertDataAsJson()`: Inserts Avro records as JSON with field name conversion
- `queryDataAsJson()`: Retrieves data using TO_JSON_STRING() for clean JSON output
- `convertToUnderscore()`: Converts camelCase to underscore_separated naming
**Logic**: Handles BigQuery's naming conventions and provides JSON-based data flow

### 4. JsonUtil Class
**Purpose**: JSON parsing and comparison utilities
**Key Methods**:
- `parseJson()`: Converts strings to JsonNode objects
- `compareJsonValues()`: Handles type-aware comparison (Double vs Integer)
- `toJson()`: Serializes objects to JSON strings
**Logic**: Provides robust JSON handling with numeric type normalization

### 5. Field Mappings Configuration
**File**: `field-mappings.properties`
**Purpose**: Externalizes field name mappings between Avro and BigQuery
**Structure**:
```properties
root.orderId=order_id
customer.customerId=customer_id
item.productId=product_id
```
**Benefits**: Maintainable, configurable field mappings without code changes

### 6. Cucumber Step Definitions
**Purpose**: BDD test orchestration and validation
**Key Steps**:
- `@Given`: Data preparation from Cucumber tables
- `@When`: Avro conversion and BigQuery insertion
- `@Then`: Data retrieval and comprehensive validation
**Logic**: End-to-end workflow with detailed logging and field-by-field validation

### 7. Validation Logic
**Purpose**: Ensures data integrity across the entire pipeline
**Features**:
- Field mapping validation (camelCase ↔ underscore_separated)
- Nested structure validation (customer, address)
- Array validation (items with multiple products)
- Type-aware comparison (handles Double/Integer mismatches)
- Detailed pass/fail reporting with expected vs actual values

## Test Architecture Benefits

1. **End-to-End Coverage**: Tests complete data flow from Cucumber → Avro → BigQuery → Validation
2. **Real BigQuery Integration**: Uses actual BigQuery tables, not mocks
3. **Field Mapping Validation**: Ensures proper conversion between naming conventions
4. **Configurable Mappings**: External properties file for easy maintenance
5. **Comprehensive Logging**: Detailed output for debugging and verification
6. **Type Safety**: Handles data type conversions and validations
7. **Unique Test Data**: Timestamp-based IDs prevent test conflicts
8. **Professional Code Quality**: Clean, maintainable, functional programming approach

## Flow

1. JSON data from Cucumber feature file
2. Convert JSON to Avro format using schema
3. Load Avro data to BigQuery
4. Retrieve data from BigQuery
5. Validate retrieved data matches original JSON