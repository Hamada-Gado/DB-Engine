package DB;

import BTree.DBBTree;

import java.io.*;
import java.util.*;


/**
 * @author ahmedgado
 */

public class Util {

    /**
     * @param tableName
     * @return Example:
     * metadata = {
     * "table1": {
     * "clusteringKey": ["id"],
     * "id": ["java.lang.Integer", "True", "index1", "BTree"],
     * "name": ["java.lang.String", "False", "index2", "RTree"]
     * },
     * "table2": {
     * "clusteringKey": ["id"],
     * "id": ["java.lang.Integer", "True", "index1", "BTree"],
     * "name": ["java.lang.String", "False", "index2", "RTree"]
     * }
     * }
     */
    public static Hashtable<String, Hashtable<String, String[]>> getMetadata(String tableName) {
        Hashtable<String, Hashtable<String, String[]>> metadata = new Hashtable<>();
        String metadataPath = DBApp.getDbConfig().getProperty("MetadataPath");

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

                    if (cKey.equals("True")) {
                        if (!metadata.get(tName).containsKey("clusteringKey")) {
                            metadata.get(tName).put("clusteringKey", new String[]{cName});
                        }
                    }

                    metadata.get(tName).put(cName, new String[]{cType, cKey, iName, iType});
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return metadata;
    }

    /**
     * Use binary search to find the page number of the record with the given clustering key
     *
     * @return a pair of (pageNumber, recordPos, found)
     * <p>
     * Example:
     * recordPos = [0, 1, 0]
     */
    public static int[] getRecordPos(String tableName, String clusteringKey,
                                     Comparable clusteringKeyValue) throws DBAppException {
        int[] recordPos = new int[3];
        Table table = Table.loadTable(tableName);

        if (table.getPagesPath().isEmpty()) {
            recordPos[1] = -1;
            return recordPos;
        }

        int leftPage = 0;
        int rightPage = table.getPagesPath().size() - 1;
        int pageNumber;

        while (true) {
            if (leftPage > rightPage) {
                pageNumber = rightPage;
                break;
            }

            int midPage = (leftPage + rightPage) / 2;
            Comparable midValue = table.getClusteringKeyMin().get(midPage);

            if (midValue.compareTo(clusteringKeyValue) <= 0) {
                // what if midValue in page of clusteringKeyValue
                // what if midValue is the last value in the page?

                // [0, 3, 5, 7, 9] , 6
                //  L  M        R  , 3 < 6
                //        L  M  R  , 7 > 6
                //        R  L
                // is this sequence always true? need more testing


                leftPage = midPage + 1;
            } else {
                if (midPage == 0) {
                    pageNumber = 0;
                    break;
                }

                rightPage = midPage - 1;
            }
        }

        Page page = table.getPage(pageNumber);
        int leftRecord = 0;
        int rightRecord = page.getRecords().size() - 1;

        while (true) {
            int midRecord = (leftRecord + rightRecord) / 2;
            Hashtable<String, Object> record = page.getRecords().get(midRecord).hashtable();
            Comparable midValue = (Comparable) record.get(clusteringKey);

            if (midValue.equals(clusteringKeyValue)) {
                recordPos[0] = pageNumber;
                recordPos[1] = midRecord;
                recordPos[2] = 1;

                return recordPos;
            } else if (leftRecord > rightRecord) {
                recordPos[0] = pageNumber;
                recordPos[1] = rightRecord;
                recordPos[2] = 0;

                return recordPos;
            } else if (midValue.compareTo(clusteringKeyValue) < 0) {
                leftRecord = midRecord + 1;
            } else {
                rightRecord = midRecord - 1;
            }
        }
    }

    public static void validateTypes(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        Hashtable<String, Hashtable<String, String[]>> metadata = getMetadata(tableName);
        if (!metadata.containsKey(tableName)) {
            throw new DBAppException("Table " + tableName + " does not exist");
        }

        for (String colName : colNameValue.keySet()) {
            if (colName.equals("clusteringKey")) {
                continue;
            }

            if (!metadata.get(tableName).containsKey(colName)) {
                throw new DBAppException("Column " + colName + " does not exist in table " + tableName);
            }

            String[] colMetadata = metadata.get(tableName).get(colName);
            String colType = colMetadata[0];

            switch (colType) {
                case "java.lang.Integer" -> {
                    if (!(colNameValue.get(colName) instanceof Integer)) {
                        throw new DBAppException("Invalid value for column " + colName + " of type " + colType);
                    }
                }
                case "java.lang.String" -> {
                    if (!(colNameValue.get(colName) instanceof String)) {
                        throw new DBAppException("Invalid value for column " + colName + " of type " + colType);
                    }
                }
                case "java.lang.Double" -> {
                    if (!(colNameValue.get(colName) instanceof Double)) {
                        throw new DBAppException("Invalid value for column " + colName + " of type " + colType);
                    }
                }
                default -> throw new DBAppException("Invalid column type " + colType);
            }
        }

    }

    public static boolean evaluateSqlTerm(Comparable value, String operator, Comparable objValue) {
        if (value == null || objValue == null) {
            return false;
        }

        return switch (operator) {
            case "=" -> value.equals(objValue);
            case "!=" -> !value.equals(objValue);
            case ">" -> (value).compareTo(objValue) > 0;
            case ">=" -> (value).compareTo(objValue) >= 0;
            case "<" -> (value).compareTo(objValue) < 0;
            case "<=" -> (value).compareTo(objValue) <= 0;
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
                    Util.evaluateSqlTerm((Comparable) value1, arrSQLTerm._strOperator, (Comparable) arrSQLTerm._objValue));

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

    public static boolean evaluatePostfix(LinkedList<Object> postfix) {
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


    // Following method returns a set of the pages of the select query using any index
    public static HashSet<Integer> filterPagesByIndex(
            SQLTerm[] arrSQLTerms,
            String[] strarrOperators, int pagesCount) throws DBAppException {

        HashSet<Integer> result = new HashSet<>();
        String tableName = arrSQLTerms[0]._strTableName;

        Hashtable<String, Hashtable<String, String[]>> metaData = Util.getMetadata(tableName);
        LinkedList<String> indexColumns = Util.getIndexColumns(metaData, tableName);

        for (String col : indexColumns) {
            String indexName = metaData.get(tableName).get(col)[2];
            DBBTree index = DBBTree.loadIndex(tableName, indexName);
            HashSet<Integer> res = new HashSet<>();
            // only consider filtering using the index if the condition is anded
            for (int i = 0; i < arrSQLTerms.length; i++) {
                if (!col.equals(arrSQLTerms[i]._strColumnName)) {
                    continue;
                }

                SQLTerm term = arrSQLTerms[i];
                String before = i == 0 ? null : strarrOperators[i - 1];
                String after = i == arrSQLTerms.length - 1 ? null : strarrOperators[i];

                if ((before == null || !before.equals("AND"))
                        && (after == null || !after.equals("AND"))) {
                    continue;
                }

                Object value = term._objValue;
                Set<Integer> search = null;

                switch (term._strOperator) {
                    case "!=" -> {
                    }
                    case "=" -> search = index.search((Comparable) value).keySet();
                    case ">", ">=" -> search = index.searchRange((Comparable) value, null);
                    case "<", "<=" -> search = index.searchRange(null, (Comparable) value);
                    default -> throw new DBAppException("Invalid operator");
                }

                if (search != null) {
                    res.addAll(search);
                }
            }

            result.addAll(res);
        }

        return result;
    }

    public static LinkedList<String> getIndexColumns(Hashtable<String, Hashtable<String, String[]>> metaData, String strTableName) {
        LinkedList<String> indexColumns = new LinkedList<>();
        //loop over metaData file and check if the index exists
        for (String colName : metaData.get(strTableName).keySet()) {
            // check if index name is not null in meta-data file
            if (colName.equals("clusteringKey")) {
                continue;
            }

            if (!metaData.get(strTableName).get(colName)[2].equals("null")) {
                indexColumns.add(colName);
            }
        }

        return indexColumns;
    }

    public static void updateIndexes(String tableName, int pageNo, int recordNo,
                                     Hashtable<String, Hashtable<String, String[]>> metadata) throws DBAppException {
        LinkedList<String> indexColumns = Util.getIndexColumns(metadata, tableName);
        Table table = Table.loadTable(tableName);
        Hashtable record = table.getPage(pageNo).getRecords().get(recordNo).hashtable();

        for (String colName : indexColumns) {
            String indexName = metadata.get(tableName).get(colName)[2];
            String indexType = metadata.get(tableName).get(colName)[3];
            if (indexType.equals("B+tree")) {
                if (record.get(colName) == null) continue;
                DBBTree tree = DBBTree.loadIndex(tableName, indexName);
                tree.insert((Comparable) record.get(colName), pageNo);
            }
        }
    }

    public static void deleteIndexes(String tableName, int pageNo, int recordNo,
                                     Hashtable<String, Hashtable<String, String[]>> metadata) throws DBAppException {
        LinkedList<String> indexColumns = Util.getIndexColumns(metadata, tableName);
        Table table = Table.loadTable(tableName);
        Hashtable record = table.getPage(pageNo).getRecords().get(recordNo).hashtable();

        for (String colName : indexColumns) {
            String indexName = metadata.get(tableName).get(colName)[2];
            String indexType = metadata.get(tableName).get(colName)[3];
            if (indexType.equals("B+tree")) {
                if (record.get(colName) == null) continue;
                DBBTree tree = DBBTree.loadIndex(tableName, indexName);
                tree.delete((Comparable) record.get(colName), pageNo);
            }
        }
    }
}
