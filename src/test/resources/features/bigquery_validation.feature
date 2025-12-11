Feature: BigQuery Data Validation
  As a data engineer
  I want to create Avro request from schema with Cucumber data and load to BigQuery
  So that I can validate data integrity

  Scenario: Create Avro request and validate in BigQuery
    Given I have complete order data from Cucumber:
      | orderId | customerId | customerName | customerEmail    | customerPhone | loyaltyTier | productId | productName | quantity | unitPrice | category    | orderDate  | street      | city    | state | zipCode | country | paymentMethod | status    | metadata     | totalAmount | taxAmount | discountApplied |
      | ORD_001 | CUST_001   | John Doe     | john@example.com | +1-555-0123   | GOLD        | PROD_001  | Laptop      | 1        | 999.99    | Electronics | 2024-01-01 | 123 Main St | Seattle | WA    | 98101   | USA     | CREDIT_CARD   | COMPLETED | {"source":"web"} | 1099.99     | 100.00    | false           |
      | ORD_002 | CUST_002   | Jane Smith   | jane@example.com | +1-555-0456   | SILVER      | PROD_002  | Mouse       | 2        | 25.50     | Electronics | 2024-01-02 | 456 Oak Ave | Portland| OR    | 97201   | USA     | PAYPAL        | PENDING   | {"source":"mobile"} | 56.10       | 5.10      | true            |
    When I create Avro request from schema using the data
    And I load the Avro request to BigQuery table "test_dataset.orders"
    Then I should retrieve the same data from BigQuery
    And the retrieved data should match the original Cucumber data