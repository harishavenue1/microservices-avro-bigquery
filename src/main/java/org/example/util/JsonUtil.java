package org.example.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class JsonUtil {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    public static JsonNode parseJson(String jsonString) throws IOException {
        return objectMapper.readTree(jsonString);
    }
    
    public static String toJson(Object object) throws IOException {
        return objectMapper.writeValueAsString(object);
    }
    
    public static boolean compareJsonValues(JsonNode expected, JsonNode actual, String fieldPath) {
        if (expected == null && actual == null) return true;
        if (expected == null || actual == null) {
            System.out.println("DEBUG: Null mismatch at " + fieldPath + " - expected: " + expected + ", actual: " + actual);
            return false;
        }
        
        if (expected.isObject() && actual.isObject()) {
            return compareJsonObjects(expected, actual, fieldPath);
        } else if (expected.isArray() && actual.isArray()) {
            return compareJsonArrays(expected, actual, fieldPath);
        } else if (expected.isNumber() && actual.isNumber()) {
            // Handle numeric comparisons (Double vs Integer)
            double expectedValue = expected.asDouble();
            double actualValue = actual.asDouble();
            boolean match = Math.abs(expectedValue - actualValue) < 0.001; // Allow small floating point differences
            if (!match) {
                System.out.println("DEBUG: Numeric value mismatch at " + fieldPath + " - expected: " + expectedValue + ", actual: " + actualValue);
            }
            return match;
        } else {
            boolean match = expected.equals(actual);
            if (!match) {
                System.out.println("DEBUG: Value mismatch at " + fieldPath + " - expected: " + expected + ", actual: " + actual);
            }
            return match;
        }
    }
    
    private static boolean compareJsonObjects(JsonNode expected, JsonNode actual, String fieldPath) {
        expected.fieldNames().forEachRemaining(fieldName -> {
            String currentPath = fieldPath.isEmpty() ? fieldName : fieldPath + "." + fieldName;
            JsonNode expectedValue = expected.get(fieldName);
            JsonNode actualValue = actual.get(fieldName);
            compareJsonValues(expectedValue, actualValue, currentPath);
        });
        return true;
    }
    
    private static boolean compareJsonArrays(JsonNode expected, JsonNode actual, String fieldPath) {
        if (expected.size() != actual.size()) {
            System.out.println("DEBUG: Array size mismatch at " + fieldPath + " - expected: " + expected.size() + ", actual: " + actual.size());
            return false;
        }
        
        for (int i = 0; i < expected.size(); i++) {
            String currentPath = fieldPath + "[" + i + "]";
            if (!compareJsonValues(expected.get(i), actual.get(i), currentPath)) {
                return false;
            }
        }
        return true;
    }
}