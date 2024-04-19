package DB;

import BTree.DBBTree;

import java.io.*;
import java.util.*;


/**
 * This class provides utility methods for the DB package.
 * It includes methods for getting metadata, evaluating SQL terms, and handling indexes.
 *
 * @author ahmedgado
 */
public class Util {

    /**
     * This method retrieves the metadata for a given table from a metadata file.
     * The metadata file is a CSV file where each line represents a column in a table.
     * Each line is split into parts: table name, column name, column type, clustering key, index name, and index type.
     * The metadata is stored in a Hashtable where the key is the table name and the value is another Hashtable.
     * The inner Hashtable has the column name as the key and an array of column properties as the value.
     * If the tableName parameter is null, the metadata for all tables is returned.
     * If the tableName parameter is not null, only the metadata for the specified table is returned.
     *
     * @param tableName The name of the table for which to retrieve the metadata. If null, the metadata for all tables is returned.
     * @return A Hashtable containing the metadata for the specified table(s). The outer Hashtable has the table name as the key and another Hashtable as the value.
     * The inner Hashtable has the column name as the key and an array of column properties (type, clustering key, index name, index type) as the value.
     * @throws RuntimeException If an error occurs while reading the metadata file.
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
     * This method uses binary search to find the position of a record with a given clustering key in a table.
     * It first loads the table and checks if it has any pages. If it doesn't, it returns an array with -1 as the second element.
     * It then performs a binary search on the pages of the table to find the page that may contain the record.
     * Once it finds the page, it performs another binary search on the records of the page to find the record.
     * If it finds the record, it sets the third element of the returned array to 1, otherwise it sets it to 0.
     * The returned array contains the page number, the position of the record in the page, and a flag indicating if the record was found.
     *
     * @param tableName          The name of the table.
     * @param clusteringKey      The clustering key.
     * @param clusteringKeyValue The value of the clustering key.
     * @return An array containing the page number, the position of the record in the page, and a flag indicating if the record was found.
     * @throws DBAppException If an error occurs while loading the table.
     */
    public static int[] getRecordPos(String tableName, String clusteringKey,
                                     Comparable<?> clusteringKeyValue) throws DBAppException {
        int[] recordPos = new int[3];
        Table<Object> table = Table.loadTable(tableName);

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
            Comparable<Object> midValue = table.getClusteringKeyMin().get(midPage);

            if (midValue.compareTo(clusteringKeyValue) <= 0) {
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
            Comparable<Object> midValue = (Comparable<Object>) record.get(clusteringKey);

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

    /**
     * This method validates the columns of a given table.
     * It first retrieves the metadata for the table. If the table does not exist, it throws a DBAppException.
     * It then iterates over the provided column names and values. For each column, it checks if the column exists in the table.
     * If the column does not exist, it throws a DBAppException.
     * It then checks the type of the value provided for the column. If the type of the value does not match the type of the column,
     * it throws a DBAppException.
     *
     * @param tableName    The name of the table.
     * @param colNameValue A Hashtable containing the column names and their values.
     * @throws DBAppException If the table does not exist, a column does not exist, or a value's type does not match the column's type.
     */
    public static void validateCols(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        Hashtable<String, Hashtable<String, String[]>> metadata = getMetadata(tableName);
        if (!metadata.containsKey(tableName)) {
            throw new DBAppException("Table " + tableName + " does not exist");
        }

        for (String colName : colNameValue.keySet()) {
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

    /**
     * This method evaluates a SQL term.
     * It first checks if either of the values is null. If either value is null, it returns false.
     * It then compares the two values based on the provided operator.
     * The operator can be one of the following: "=", "!=", ">", ">=", "<", "<=".
     * If the operator is "=", it checks if the two values are equal.
     * If the operator is "!=", it checks if the two values are not equal.
     * If the operator is ">", it checks if the first value is greater than the second value.
     * If the operator is ">=", it checks if the first value is greater than or equal to the second value.
     * If the operator is "<", it checks if the first value is less than the second value.
     * If the operator is "<=", it checks if the first value is less than or equal to the second value.
     * If the operator is not one of the above, it throws a RuntimeException.
     *
     * @param value    The first value to be compared.
     * @param operator The operator to be used for comparison.
     * @param objValue The second value to be compared.
     * @return True if the SQL term is true, false otherwise.
     * @throws RuntimeException If an invalid operator is provided.
     */
    public static boolean evaluateSqlTerm(Comparable<Object> value, String operator, Comparable<Object> objValue) {
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
     * This method compares the precedence of two SQL operators.
     * It supports the following operators: "AND", "OR", "XOR".
     * The precedence order is as follows: "AND" > "OR" > "XOR".
     * If the precedence of the first operator is higher than or equal to the second operator, it returns true.
     * Otherwise, it returns false.
     * If an invalid operator is provided, it throws a RuntimeException.
     *
     * @param op  The first operator.
     * @param op2 The second operator.
     * @return True if the precedence of the first operator is higher than or equal to the second operator, false otherwise.
     * @throws RuntimeException If an invalid operator is provided.
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

    /**
     * This method converts an array of SQL terms and operators into postfix notation.
     * It iterates over the SQL terms and evaluates each term using the evaluateSqlTerm method.
     * The result of the evaluation is added to the postfix list.
     * It then checks if there are any remaining operators.
     * If there are, it compares the precedence of the current operator with the operator at the top of the stack.
     * If the current operator has higher or equal precedence, it pops the operator from the stack and adds it to the postfix list.
     * The current operator is then pushed onto the stack.
     * After all terms and operators have been processed, any remaining operators on the stack are popped and added to the postfix list.
     *
     * @param record          The record to be evaluated.
     * @param arrSQLTerms     The array of SQL terms.
     * @param strarrOperators The array of operators.
     * @return A LinkedList containing the postfix notation of the SQL terms and operators.
     */
    public static LinkedList<Object> toPostfix(Hashtable<String, Object> record,
                                               SQLTerm[] arrSQLTerms, String[] strarrOperators) {
        Stack<String> stack = new Stack<>();
        LinkedList<Object> postfix = new LinkedList<>();
        int j = 0;

        for (SQLTerm arrSQLTerm : arrSQLTerms) {
            Object value1 = record.get(arrSQLTerm._strColumnName);
            postfix.add(
                    Util.evaluateSqlTerm((Comparable<Object>) value1, arrSQLTerm._strOperator, (Comparable<Object>) arrSQLTerm._objValue));

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

    /**
     * This method evaluates a postfix notation of SQL terms and operators.
     * It iterates over the postfix list. If the current element is a Boolean, it pushes it onto the stack.
     * If the current element is an operator, it pops two values from the stack, evaluates the operation, and pushes the result back onto the stack.
     * After all elements in the postfix list have been processed, the method returns the final result from the top of the stack.
     *
     * @param postfix The postfix notation of SQL terms and operators.
     * @return The result of the evaluation.
     */
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

    /**
     * This method evaluates a binary operation.
     * It supports the following operators: "AND", "OR", "XOR".
     * If the operator is "AND", it returns the logical AND of the two values.
     * If the operator is "OR", it returns the logical OR of the two values.
     * If the operator is "XOR", it returns the logical XOR of the two values.
     * If the operator is not one of the above, it throws a RuntimeException.
     *
     * @param value    The first value.
     * @param operator The operator.
     * @param objValue The second value.
     * @return The result of the binary operation.
     * @throws RuntimeException If an invalid operator is provided.
     */
    public static boolean evaluateBinaryOp(boolean value, String operator, boolean objValue) {
        return switch (operator) {
            case "AND" -> value && objValue;
            case "OR" -> value || objValue;
            case "XOR" -> value ^ objValue;
            default -> throw new RuntimeException("Invalid operator");
        };
    }

    /**
     * This method returns a set of the pages of the select query using any available index.
     * It first retrieves the metadata for the table and gets the index columns.
     * It then iterates over the index columns and loads the corresponding index.
     * For each SQL term, it checks if the column name matches the current index column and if the condition is ANDed.
     * If the condition is not ANDed, it continues to the next term.
     * It then performs a search on the index based on the operator and value of the term.
     * The result of the search is added to a set of page numbers.
     * If the overall result set is empty, it adds all page numbers from the current result set.
     * Otherwise, it retains only the page numbers that are present in both the overall result set and the current result set.
     * After all terms have been processed, it returns the overall result set.
     *
     * @param arrSQLTerms     The array of SQL terms.
     * @param strarrOperators The array of operators.
     * @return A HashSet containing the page numbers that satisfy the select query.
     * @throws DBAppException If an invalid operator is provided.
     */
    public static HashSet<Integer> filterPagesByIndex(
            SQLTerm[] arrSQLTerms,
            String[] strarrOperators) throws DBAppException {

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

                if (result.isEmpty()) {
                    result.addAll(res);
                } else {
                    result.retainAll(res);
                }
            }
        }

        return result;
    }

    /**
     * This method retrieves the index columns for a given table from the metadata.
     * It iterates over the metadata for the table and checks if each column has an index.
     * If a column has an index, it adds the column name to a list of index columns.
     * After all columns have been processed, it returns the list of index columns.
     *
     * @param metaData     The metadata for the table.
     * @param strTableName The name of the table.
     * @return A LinkedList containing the names of the index columns.
     */
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

    /**
     * This method updates the indexes of a given table.
     * It first retrieves the metadata for the table and gets the index columns.
     * It then loads the table and retrieves the record at the specified page number and record number.
     * It iterates over the index columns and checks if the index type is "B+tree".
     * If the index type is "B+tree", it loads the index and inserts the value of the column in the record into the index.
     * The page number is used as the key for the index.
     *
     * @param tableName The name of the table.
     * @param pageNo    The page number of the record.
     * @param recordNo  The record number within the page.
     * @throws DBAppException If an error occurs while loading the table or the index.
     */
    public static void updateIndexes(String tableName, int pageNo, int recordNo) throws DBAppException {
        Hashtable<String, Hashtable<String, String[]>> metadata = Util.getMetadata(tableName);
        LinkedList<String> indexColumns = Util.getIndexColumns(metadata, tableName);
        Table<Object> table = Table.loadTable(tableName);
        Hashtable<String, Object> record = table.getPage(pageNo).getRecords().get(recordNo).hashtable();

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

    /**
     * This method deletes the indexes of a given table.
     * It first retrieves the metadata for the table and gets the index columns.
     * It then loads the table and retrieves the record at the specified page number and record number.
     * It iterates over the index columns and checks if the index type is "B+tree".
     * If the index type is "B+tree", it loads the index and deletes the value of the column in the record from the index.
     * The page number is used as the key for the index.
     *
     * @param tableName The name of the table.
     * @param pageNo    The page number of the record.
     * @param recordNo  The record number within the page.
     * @throws DBAppException If an error occurs while loading the table or the index.
     */
    public static void deleteIndexes(String tableName, int pageNo, int recordNo) throws DBAppException {
        Hashtable<String, Hashtable<String, String[]>> metadata = Util.getMetadata(tableName);
        LinkedList<String> indexColumns = Util.getIndexColumns(metadata, tableName);
        Table<Object> table = Table.loadTable(tableName);
        Hashtable<String, Object> record = table.getPage(pageNo).getRecords().get(recordNo).hashtable();

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

    /**
     * This method recreates the indexes of a given table.
     * It first retrieves the metadata for the table and gets the index columns.
     * It then iterates over the index columns. For each column, it checks if the index type is "B+tree".
     * If the index type is "B+tree", it calls the createIndex method of the DBApp instance to create the index.
     * The table name, column name, and index name are passed as parameters to the createIndex method.
     *
     * @param tableName The name of the table.
     * @param dbApp     The DBApp instance.
     * @throws DBAppException If an error occurs while creating the index.
     */
    public static void recreateIndexes(String tableName, DBApp dbApp) throws DBAppException {
        Hashtable<String, Hashtable<String, String[]>> metadata = Util.getMetadata(tableName);
        LinkedList<String> indexColumns = Util.getIndexColumns(metadata, tableName);

        for (String colName : indexColumns) {
            String indexName = metadata.get(tableName).get(colName)[2];
            String indexType = metadata.get(tableName).get(colName)[3];
            if (indexType.equals("B+tree")) {
                dbApp.createIndex(tableName, colName, indexName);
            }
        }
    }
}
