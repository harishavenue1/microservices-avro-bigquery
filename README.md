# Microservices Avro-BigQuery Project

This project demonstrates JSON creation based on Avro schema and BigQuery integration with validation using Cucumber tests.

## Architecture Overview

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐    ┌──────────────┐
│   Cucumber      │    │ Avro Schema  │    │   BigQuery      │    │  Validation  │
│   Test Data     │───▶│ JSON Creation│───▶│   Insertion     │───▶│   Engine     │
│                 │    │              │    │                 │    │              │
└─────────────────┘    └──────────────┘    └─────────────────┘    └──────────────┘
         │                       │                    │                     │
         ▼                       ▼                    ▼                     ▼
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐    ┌──────────────┐
│ JSON Structure  │    │ GenericRecord│    │ Nested STRUCT   │    │ Field-by-Field│
│ • Customer      │    │ • Schema     │    │ • Arrays        │    │ • Expected vs │
│ • Items[]       │    │ • Compliance │    │ • Type Safety   │    │   Actual     │
│ • Address       │    │ • Type Cast  │    │ • JSON Output   │    │ • PASS/FAIL  │
└─────────────────┘    └──────────────┘    └─────────────────┘    └──────────────┘
```

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
- `src/test/resources/field-mappings.properties` - **Field mapping configuration (see detailed explanation below)**

## Field Mapping Configuration

The `field-mappings.properties` file controls the validation order and field mappings between JSON and BigQuery. It supports three formats:

### 1. Simple Field Mapping
```properties
# Format: fieldName=bigQueryFieldName
orderId=order_id
orderDate=order_date
paymentMethod=payment_method
status=status
metadata=metadata
totalAmount=total_amount
taxAmount=tax_amount
discountApplied=discount_applied
```

### 2. Nested Object Mapping
```properties
# Format: fieldName=[subField1=bqField1,subField2=bqField2,...]
customer=[customerId=customer_id,name=name,email=email,phone=phone,loyaltyTier=loyalty_tier]
```

### 3. Nested Object with Custom BigQuery Field
```properties
# Format: fieldName=bqFieldName:{subField1=bqField1,subField2=bqField2,...}
shippingAddress=shipping_address:{street=street,city=city,state=state,zipCode=zip_code,country=country}
```

### 4. Array Mapping
```properties
# Format: fieldName=[itemField1=bqField1,itemField2=bqField2,...]
items=[productId=product_id,productName=product_name,quantity=quantity,unitPrice=unit_price,category=category]
```

### Complete Configuration Example
```properties
# Validation order with embedded field mappings
orderId=order_id
customer=[customerId=customer_id,name=name,email=email,phone=phone,loyaltyTier=loyalty_tier]
items=[productId=product_id,productName=product_name,quantity=quantity,unitPrice=unit_price,category=category]
orderDate=order_date
shippingAddress=shipping_address:{street=street,city=city,state=state,zipCode=zip_code,country=country}
paymentMethod=payment_method
status=status
metadata=metadata
totalAmount=total_amount
taxAmount=tax_amount
discountApplied=discount_applied
```

### Key Features:
- **Order Preservation**: Fields are validated in the exact order they appear in the file
- **Automatic Detection**: Engine automatically detects simple fields, objects, and arrays
- **Flexible Mapping**: Supports different BigQuery field names using `bqField:{}` syntax
- **Nested Support**: Handles complex nested structures and arrays
- **Maintainable**: All configuration in one file, no code changes needed for new fields

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

#### Detailed Validation Flow
```
=== Starting Record Validation ===

Validating field: orderId with config: order_id
  -> Simple field detected
DEBUG: Comparing field: orderId
  Expected type: STRING, value: "ORD001_1703123456789"
  Actual type: STRING, value: "ORD001_1703123456789"
  -> Direct value comparison
  -> Expected: "ORD001_1703123456789", Actual: "ORD001_1703123456789", match: true
orderId -> order_id: PASS | Expected: "ORD001_1703123456789" | Actual: "ORD001_1703123456789"

Validating field: customer with config: [customerId=customer_id,name=name,email=email,phone=phone,loyaltyTier=loyalty_tier]
  -> Nested field detected
    Using field name as BigQuery field: customer
    Field mappings: customerId=customer_id,name=name,email=email,phone=phone,loyaltyTier=loyalty_tier
    Null check - Original: true, Retrieved: true
    -> Object type detected
      Object validation - mapping: customerId -> customer_id
DEBUG: Comparing field: customerId
  Expected type: STRING, value: "CUST001"
  Actual type: STRING, value: "CUST001"
  -> Direct value comparison
  -> Expected: "CUST001", Actual: "CUST001", match: true
Customer customerId -> customer_id: PASS | Expected: "CUST001" | Actual: "CUST001"

      Object validation - mapping: name -> name
DEBUG: Comparing field: name
  Expected type: STRING, value: "John Doe"
  Actual type: STRING, value: "John Doe"
  -> Direct value comparison
  -> Expected: "John Doe", Actual: "John Doe", match: true
Customer name: PASS | Expected: "John Doe" | Actual: "John Doe"

Validating field: items with config: [productId=product_id,productName=product_name,quantity=quantity,unitPrice=unit_price,category=category]
  -> Nested field detected
    Using field name as BigQuery field: items
    Field mappings: productId=product_id,productName=product_name,quantity=quantity,unitPrice=unit_price,category=category
    Null check - Original: true, Retrieved: true
    -> Array type detected, size: 1
      Array validation - mapping: productId -> product_id
DEBUG: Comparing field: productId
  Expected type: STRING, value: "PROD001"
  Actual type: STRING, value: "PROD001"
  -> Direct value comparison
  -> Expected: "PROD001", Actual: "PROD001", match: true
item[0] productId -> product_id: PASS | Expected: "PROD001" | Actual: "PROD001"

      Array validation - mapping: unitPrice -> unit_price
DEBUG: Comparing field: unitPrice
  Expected type: NUMBER, value: 999.99
  Actual type: NUMBER, value: 999.99
  -> Numeric comparison
  -> Expected: 999.99, Actual: 999.99, match: true
item[0] unitPrice -> unit_price: PASS | Expected: 999.99 | Actual: 999.99

Validating field: shippingAddress with config: shipping_address:{street=street,city=city,state=state,zipCode=zip_code,country=country}
  -> Nested field detected
    BigQuery field: shipping_address, Mappings: {street=street,city=city,state=state,zipCode=zip_code,country=country}
    Field mappings: street=street,city=city,state=state,zipCode=zip_code,country=country
    Null check - Original: true, Retrieved: true
    -> Object type detected
      Object validation - mapping: zipCode -> zip_code
DEBUG: Comparing field: zipCode
  Expected type: STRING, value: "10001"
  Actual type: STRING, value: "10001"
  -> Direct value comparison
  -> Expected: "10001", Actual: "10001", match: true
ShippingAddress zipCode -> zip_code: PASS | Expected: "10001" | Actual: "10001"

=== Record Validation Complete ===
```

#### Summary Output
```
orderId -> order_id: PASS | Expected: "ORD001_1703123456789" | Actual: "ORD001_1703123456789"
Customer customerId -> customer_id: PASS | Expected: "CUST001" | Actual: "CUST001"
Customer name: PASS | Expected: "John Doe" | Actual: "John Doe"
Customer email: PASS | Expected: "john@example.com" | Actual: "john@example.com"
Customer phone: PASS | Expected: "555-1234" | Actual: "555-1234"
Customer loyaltyTier -> loyalty_tier: PASS | Expected: "Gold" | Actual: "Gold"
item[0] productId -> product_id: PASS | Expected: "PROD001" | Actual: "PROD001"
item[0] productName -> product_name: PASS | Expected: "Laptop" | Actual: "Laptop"
item[0] quantity: PASS | Expected: 2 | Actual: 2
item[0] unitPrice -> unit_price: PASS | Expected: 999.99 | Actual: 999.99
item[0] category: PASS | Expected: "Electronics" | Actual: "Electronics"
orderDate -> order_date: PASS | Expected: "2023-12-01" | Actual: "2023-12-01"
ShippingAddress street: PASS | Expected: "123 Main St" | Actual: "123 Main St"
ShippingAddress city: PASS | Expected: "New York" | Actual: "New York"
ShippingAddress state: PASS | Expected: "NY" | Actual: "NY"
ShippingAddress zipCode -> zip_code: PASS | Expected: "10001" | Actual: "10001"
ShippingAddress country: PASS | Expected: "USA" | Actual: "USA"
paymentMethod -> payment_method: PASS | Expected: "Credit Card" | Actual: "Credit Card"
status: PASS | Expected: "Shipped" | Actual: "Shipped"
metadata: PASS | Expected: "Priority Order" | Actual: "Priority Order"
totalAmount -> total_amount: PASS | Expected: 1999.98 | Actual: 1999.98
taxAmount -> tax_amount: PASS | Expected: 159.99 | Actual: 159.99
discountApplied -> discount_applied: PASS | Expected: true | Actual: true
```

## BigQuery Results
<img width="1398" height="857" alt="image" src="https://github.com/user-attachments/assets/18186b8f-bc6c-439b-bfea-2707abbeda60" />

**Query Results Table:**
```
| order_id              | customer.customer_id | customer.name | customer.email      | customer.phone | customer.loyalty_tier | items.product_id |
|-----------------------|---------------------|---------------|---------------------|----------------|----------------------|------------------|
| ORD_001_1765425681089 | CUST_001           | John Doe      | john@example.com    | +1-555-0123    | GOLD                 | PROD_001         |
| ORD_002_1765425681089 | CUST_002           | Jane Smith    | jane@example.com    | +1-555-0456    | SILVER               | PROD_002         |
| ORD_001_1765426661198 | CUST_001           | John Doe      | john@example.com    | +1-555-0123    | GOLD                 | PROD_001         |
| ORD_002_1765426661198 | CUST_002           | Jane Smith    | jane@example.com    | +1-555-0456    | SILVER               | PROD_002         |
```

*BigQuery successfully stores and queries nested structures with proper field mapping validation. The timestamp-based order IDs demonstrate multiple successful test runs.*

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
2. Create Avro GenericRecord based on schema structure
3. Load Avro data to BigQuery as JSON
4. Retrieve data from BigQuery
5. Validate retrieved data matches original JSON
