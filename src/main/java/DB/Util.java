package DB;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;


/**
 * @author ahmedgado
 */

public class Util {
    public static Hashtable<String, Hashtable<String, String[]>> getMetadata(String tableName) {
        if (DBApp.db_config == null) {
            throw new RuntimeException("DBApp not initialized");
        }

        Hashtable<String, Hashtable<String, String[]>> metadata = new Hashtable<>();
        String metadataPath = DBApp.db_config.getProperty("MetadataPath");

        try (BufferedReader br = new BufferedReader(new FileReader(metadataPath))) {
            br.readLine(); // Skip the header
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                String tName = parts[0];
                String cName = parts[1];
                String cType = parts[2];
                String cKey = parts[3];
                String iName = parts[4];
                String iType = parts[5];

                if (tableName == null || tName.equals(tableName)) {
                    if (!metadata.containsKey(tName)) {
                        metadata.put(tName, new Hashtable<>());
                    }

                    metadata.get(tName).put(cName, new String[]{cType, cKey, iName, iType});
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return metadata;
    }

    public static boolean evaluateSqlTerm(Object value, Object operator, Object objValue) {
        if (value == null || objValue == null) {
            return false;
        }

        return switch ((String) operator) {
            case "=" -> value.equals(objValue);
            case "!=" -> !value.equals(objValue);
            case ">" -> ((Comparable) value).compareTo(objValue) > 0;
            case ">=" -> ((Comparable) value).compareTo(objValue) >= 0;
            case "<" -> ((Comparable) value).compareTo(objValue) < 0;
            case "<=" -> ((Comparable) value).compareTo(objValue) <= 0;
            default -> throw new RuntimeException("Invalid operator");
        };
    }

    /**
     * Compares the precedence of two operators
     *
     * @param op  the first operator
     * @param op2 the second operator
     * @return true if the precedence of op is higher than or equal to op2, false otherwise
     */
    public static boolean comparePrecedence(String op, String op2) {
        return switch (op) {
            case "AND" -> switch (op2) {
                case "AND" -> true;
                case "OR", "XOR" -> false;
                default -> throw new RuntimeException("Invalid operator");
            };
            case "OR" -> switch (op2) {
                case "AND", "OR" -> true;
                case "XOR" -> false;
                default -> throw new RuntimeException("Invalid operator");
            };
            case "XOR" -> switch (op2) {
                case "AND", "OR", "XOR" -> true;
                default -> throw new RuntimeException("Invalid operator");
            };
            default -> throw new RuntimeException("Invalid operator");
        };
    }

    public static LinkedList<Object> toPostfix(Hashtable<String, Object> record,
                                               SQLTerm[] arrSQLTerms, String[] strarrOperators) {

        Stack<String> stack = new Stack<>();
        LinkedList<Object> postfix = new LinkedList<>();
        int j = 0;

        for (SQLTerm arrSQLTerm : arrSQLTerms) {
            Object value1 = record.get(arrSQLTerm._strColumnName);
            postfix.add(
                    Util.evaluateSqlTerm(value1, arrSQLTerm._strOperator, arrSQLTerm._objValue));

            if (j >= strarrOperators.length) {
                continue;
            }

            while (!stack.isEmpty() && Util.comparePrecedence(strarrOperators[j], stack.peek())) {
                postfix.add(stack.pop());
            }
            stack.push(strarrOperators[j]);
            j++;
        }

        while (!stack.isEmpty()) {
            postfix.add(stack.pop());
        }

        return postfix;
    }

    public static boolean evaluatePostfix(LinkedList postfix) {
        Stack<Boolean> stack = new Stack<>();
        for (Object token : postfix) {
            if (token instanceof Boolean) {
                stack.push((Boolean) token);
            } else {
                boolean value2 = stack.pop();
                boolean value1 = stack.pop();
                stack.push(Util.evaluateBinaryOp(value1, (String) token, value2));
            }
        }
        return stack.pop();
    }

    public static boolean evaluateBinaryOp(boolean value, String operator, boolean objValue) {

        return switch (operator) {
            case "AND" -> value && objValue;
            case "OR" -> value || objValue;
            case "XOR" -> value ^ objValue;
            default -> throw new RuntimeException("Invalid operator");
        };
    }
}