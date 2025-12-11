# Microservices Avro-BigQuery Project

This project demonstrates JSON to Avro conversion and BigQuery integration with validation using Cucumber tests.

## Setup

1. **Google Cloud Setup**:
   - Create a Google Cloud project
   - Enable BigQuery API
   - Create service account credentials
   - Set `GOOGLE_APPLICATION_CREDENTIALS` environment variable

2. **Configuration**:
   - Update `src/main/resources/application.properties` with your project ID

3. **Build**:
   ```bash
   mvn clean compile
   ```

4. **Run Tests**:
   ```bash
   mvn test
   ```

## Project Structure

- `src/main/avro/` - Avro schema files
- `src/main/java/org/example/util/` - Utility classes for Avro and BigQuery
- `src/test/resources/features/` - Cucumber feature files
- `src/test/java/org/example/steps/` - Cucumber step definitions

## Flow

1. JSON data from Cucumber feature file
2. Convert JSON to Avro format using schema
3. Load Avro data to BigQuery
4. Retrieve data from BigQuery
5. Validate retrieved data matches original JSON