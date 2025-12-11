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
        System.out.println("DEBUG: Comparing field: " + fieldPath);
        System.out.println("  Expected type: " + (expected != null ? expected.getNodeType() : "null") + ", value: " + expected);
        System.out.println("  Actual type: " + (actual != null ? actual.getNodeType() : "null") + ", value: " + actual);
        
        if (expected == null && actual == null) {
            System.out.println("  -> Both null, match: true");
            return true;
        }
        if (expected == null || actual == null) {
            System.out.println("  -> Null mismatch, match: false");
            return false;
        }
        
        if (expected.isObject() && actual.isObject()) {
            System.out.println("  -> Object comparison");
            return compareJsonObjects(expected, actual, fieldPath);
        } else if (expected.isArray() && actual.isArray()) {
            System.out.println("  -> Array comparison");
            return compareJsonArrays(expected, actual, fieldPath);
        } else if (expected.isNumber() && actual.isNumber()) {
            System.out.println("  -> Numeric comparison");
            // Handle numeric comparisons (Double vs Integer)
            double expectedValue = expected.asDouble();
            double actualValue = actual.asDouble();
            boolean match = Math.abs(expectedValue - actualValue) < 0.001; // Allow small floating point differences
            System.out.println("  -> Expected: " + expectedValue + ", Actual: " + actualValue + ", match: " + match);
            return match;
        } else {
            System.out.println("  -> Direct value comparison");
            boolean match = expected.equals(actual);
            System.out.println("  -> Expected: " + expected + ", Actual: " + actual + ", match: " + match);
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