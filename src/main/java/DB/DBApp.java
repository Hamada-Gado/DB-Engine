package DB;

import BTree.DBBTree;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * @author Wael Abouelsaadat
 */

public class DBApp {

    public static final String configPath = "src/main/resources/DBApp.config";
    public static final String metadataHeader = "Table Name,Column Name,Column Type,ClusteringKey,IndexName,IndexType\n";
    private static Properties db_config;

    public DBApp() {
        this.init();
    }

    /**
     * This method is used for initialization at the application startup.
     * It performs the following operations:
     * 1. Reads the configuration file and loads it into a Properties object.
     * 2. Checks if the data folder exists. If it doesn't, it creates the data folder.
     * 3. Checks if the metadata file exists. If it doesn't, it creates the metadata file and writes the metadata header into it.
     *
     * @throws RuntimeException If an error occurs while reading the configuration file, creating the data folder, creating the metadata file, or writing the metadata header.
     */
    public void init() {
        // Read the config file
        try (FileReader reader = new FileReader(configPath)) {
            db_config = new Properties();
            db_config.load(reader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Create the data folder if it doesn't exist
        File dataFolder = new File(getDbConfig().getProperty("DataPath"));
        if (!dataFolder.exists()) {
            boolean newDir = dataFolder.mkdirs();
            if (!newDir) {
                throw new RuntimeException("Couldn't make data folder");
            }
        }

        // Create the metadata folder if it doesn't exist
        File metadataFile = new File(getDbConfig().getProperty("MetadataPath"));
        if (!metadataFile.exists()) {
            try {
                boolean newFile = metadataFile.createNewFile();
                if (!newFile) {
                    throw new RuntimeException("Couldn't make metadata file");
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            // Add the metadata header
            try (FileWriter writer = new FileWriter(metadataFile)) {
                writer.write(metadataHeader);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * This method is used to create a new table in the database.
     * It first checks if the table name, clustering key column, and column name type are not null.
     * It then validates the data type of each column.
     * If the data type is not one of the supported types (Integer, Double, String), it throws an exception.
     * It then creates a new Table object and a new directory for the table.
     * If the directory already exists, it throws an exception.
     * It then updates the metadata file with the information about the new table.
     * The clustering key column is marked as "True" in the metadata file.
     * Finally, it saves the Table object to disk.
     *
     * @param strTableName           The name of the new table.
     * @param strClusteringKeyColumn The name of the clustering key column.
     * @param htblColNameType        A Hashtable mapping column names to their data types.
     * @throws DBAppException If the table already exists, or if an error occurs while creating the table directory or updating the metadata file.
     */
    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String, String> htblColNameType) throws DBAppException {
        if (strTableName == null || strClusteringKeyColumn == null || htblColNameType == null) {
            throw new DBAppException("Null arguments");
        }

        for (String colName : htblColNameType.keySet()) {
            if (!htblColNameType.get(colName).equals("java.lang.Integer") &&
                    !htblColNameType.get(colName).equals("java.lang.Double") &&
                    !htblColNameType.get(colName).equals("java.lang.String")
            ) {
                throw new DBAppException("Invalid column type");
            }
        }

        if (!htblColNameType.containsKey(strClusteringKeyColumn)) {
            throw new DBAppException("Clustering Key is not given as input");
        }

        String metadataPath = getDbConfig().getProperty("MetadataPath");

        // create a new table, and parent folder
        Table<Object> table = new Table<>(strTableName);
        Path tablePath = Paths.get((String) getDbConfig().get("DataPath"), strTableName);
        File file = new File(tablePath.toAbsolutePath().toString());
        if (!file.exists()) {
            boolean newDir = file.mkdirs();
            if (!newDir) {
                throw new RuntimeException("Couldn't make table folder");
            }
        } else {
            throw new DBAppException("Table already exists");
        }

        // update metadata, and set clustering key
        try (FileWriter writer = new FileWriter(metadataPath, true)) {
            for (String colName : htblColNameType.keySet()) {
                String colType = htblColNameType.get(colName);
                String clusteringKey = colName.equals(strClusteringKeyColumn) ? "True" : "False";
                writer.write(strTableName + "," + colName + "," + colType + "," + clusteringKey + ",null,null\n");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // save table to disk
        Path path = Paths.get((String) getDbConfig().get("DataPath"), strTableName, strTableName + ".ser");
        try (
                FileOutputStream fileOut = new FileOutputStream(path.toAbsolutePath().toString());
                ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
            out.writeObject(table);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This method is used to create a B+ tree index for a specific column in a table.
     * It first checks if the table name, column name, and index name are not null.
     * It then loads the table from the disk and creates a new B+ tree.
     * It iterates over all the records in the table and inserts the value of the column and the record's key into the B+ tree.
     * It then saves the B+ tree to the disk.
     * Finally, it updates the metadata file with the information about the new index.
     *
     * @param strTableName The name of the table.
     * @param strColName   The name of the column.
     * @param strIndexName The name of the index.
     * @throws DBAppException If the table name, column name, or index name is null, or if an error occurs while writing to the metadata file.
     */
    public void createIndex(String strTableName,
                            String strColName,
                            String strIndexName) throws DBAppException {
        if (strTableName == null || strColName == null || strIndexName == null) {
            throw new DBAppException("Null arguments");
        }

        // Load the table from the disk
        Table<Object> table = Table.loadTable(strTableName);

        // Create a new B+ tree
        DBBTree bpt = new DBBTree(strTableName, strIndexName);

        // Iterate over all the records in the table
        for (int i = 0; i < table.pagesCount(); i++) {
            Page page = table.getPage(i);
            for (Record record : page.getRecords()) {
                // Insert the value of the column and the record's key into the B+ tree
                bpt.insert((Comparable) record.hashtable().get(strColName), i);
            }
        }

        // Save the B+ tree to the disk
        bpt.saveIndex();

        // write to metadata
        String metadataPath = getDbConfig().getProperty("MetadataPath");
        LinkedList<String> metadataString = new LinkedList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(metadataPath))) {
            br.readLine();
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                String tName = parts[0];
                String cName = parts[1];
                String cType = parts[2];
                String cKey = parts[3];

                if (tName.equals(strTableName) && cName.equals(strColName)) {
                    metadataString.add(strTableName + "," + strColName + "," + cType + ","
                            + cKey + "," + strIndexName + ",B+tree\n");
                } else {
                    metadataString.add(line + "\n");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try (FileWriter writer = new FileWriter(metadataPath, false)) {
            writer.write(DBApp.metadataHeader);
            for (String line : metadataString) {
                writer.write(line);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This method is used to insert a new record into a table.
     * It first checks if the table name and the record are not null.
     * It then validates the columns of the record.
     * It retrieves the metadata for the table and gets the clustering key.
     * If the record does not contain a value for the clustering key, it throws an exception.
     * It then loads the table from the disk and gets the position of the record.
     * If the record already exists in the table, it throws an exception.
     * It then iterates over the pages of the table. For each page, it adds the record and updates the indexes.
     * If the page is full, it removes the last record from the page and deletes its indexes.
     * The removed record is then inserted into the next page.
     * If there are no more pages, it creates a new page and inserts the record into it.
     *
     * @param strTableName     The name of the table.
     * @param htblColNameValue A Hashtable mapping column names to their values.
     * @throws DBAppException If the table name or the record is null, if the record does not contain a value for the clustering key, or if the record already exists in the table.
     */
    public void insertIntoTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue) throws DBAppException {
        if (strTableName == null || htblColNameValue == null) {
            throw new DBAppException(("No value being inserted"));
        }

        Util.validateCols(strTableName, htblColNameValue);

        Hashtable<String, Hashtable<String, String[]>> metaData = Util.getMetadata(strTableName);
        if (metaData.get(strTableName) == null) {
            throw new DBAppException("Table not found");
        }

        String pKey = metaData.get(strTableName).get("clusteringKey")[0];
        if (!htblColNameValue.containsKey(pKey)) {
            throw new DBAppException("Primary key not found");
        }
        Comparable<Object> pValue = (Comparable<Object>) htblColNameValue.get(pKey);

        Table<Object> currentTable = Table.loadTable(strTableName);

        int[] recordPos = Util.getRecordPos(strTableName, pKey, pValue);

        if (recordPos[2] == 1) {
            throw new DBAppException("Record with the following primary key already exist: (" + pKey + ") " + pValue);
        }

        int pageNo = recordPos[0];
        int recordNo = recordPos[1];

        for (int currentPageNo = pageNo; currentPageNo <= currentTable.pagesCount(); currentPageNo++) {
            if (currentPageNo < currentTable.pagesCount()) {
                Page page = currentTable.getPage(currentPageNo);
                currentTable.addRecord(recordNo + 1, new Record(htblColNameValue), pKey, page);
                Util.updateIndexes(strTableName, currentPageNo, recordNo + 1);
                if (page.size() == page.getMax() + 1) {
                    Util.deleteIndexes(strTableName, currentPageNo, page.getMax());
                    htblColNameValue = currentTable.removeRecord(page.getMax(), pKey, page).hashtable();
                    recordNo = -1;
                } else {
                    break;
                }
            } else {
                Page newPage = currentTable.addPage(Integer.parseInt((String) DBApp.getDbConfig().get("MaximumRowsCountinPage")));
                currentTable.addRecord(new Record(htblColNameValue), pKey, newPage);
                Util.updateIndexes(strTableName, currentPageNo, recordNo + 1);
                break;
            }
        }
    }

    /**
     * This method is used to update a specific record in a table.
     * It first checks if the table name, clustering key value, and the record are not null.
     * It then validates the columns of the record.
     * It retrieves the metadata for the table and gets the clustering key.
     * It then loads the table from the disk and gets the position of the record.
     * If the record does not exist in the table, it throws an exception.
     * It then updates the record in the table and saves the page to the disk.
     * Finally, it updates the indexes of the table.
     *
     * @param strTableName          The name of the table.
     * @param strClusteringKeyValue The value of the clustering key of the record to be updated.
     * @param htblColNameValue      A Hashtable mapping column names to their new values.
     * @throws DBAppException If the table name, clustering key value, or the record is null, if the record does not exist in the table, or if an error occurs while updating the record.
     */
    public void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String, Object> htblColNameValue) throws DBAppException {
        if (strTableName == null || strClusteringKeyValue == null || htblColNameValue == null) {
            throw new DBAppException("Null arguments");
        }

        if (htblColNameValue.isEmpty()) {
            throw new DBAppException("No value being updated");
        }

        Util.validateCols(strTableName, htblColNameValue);

        Table<Object> table = Table.loadTable(strTableName);
        Hashtable<String, Hashtable<String, String[]>> metaData = Util.getMetadata(strTableName);

        // check if the table exists
        if (metaData.get(strTableName) == null) {
            throw new DBAppException("Table does not exist");
        }
        Object clusteringKeyValue;

        String compare = metaData.get(strTableName).get("clusteringKey")[0];
        String clustKeyType = metaData.get(strTableName).get(compare)[0];
        if (clustKeyType.equals("java.lang.Integer")) {
            clusteringKeyValue = Integer.parseInt(strClusteringKeyValue);
        } else if (clustKeyType.equals("java.lang.Double")) {
            clusteringKeyValue = Double.parseDouble(strClusteringKeyValue);
        } else {
            clusteringKeyValue = strClusteringKeyValue;
        }
        String pKey = metaData.get(strTableName).get("clusteringKey")[0];
        int[] info = Util.getRecordPos(strTableName, pKey, (Comparable) clusteringKeyValue);
        Util.deleteIndexes(strTableName, info[0], info[1]);

        if (info[2] == 0) {
            throw new DBAppException("Record Not found");
        }

        Page page = table.getPage(info[0]);
        Vector<Record> records = page.getRecords();
        Record record = records.get(info[1]);

        for (String colName : htblColNameValue.keySet()) {
            record.hashtable().put(colName, htblColNameValue.get(colName));
        }
        page.savePage();
        Util.updateIndexes(strTableName, info[0], info[1]);
    }

    /**
     * This method is used to delete one or more records from a table.
     * It first checks if the table name and the record are not null.
     * If the record is empty, it deletes all files in the table folder and clears the table.
     * It then validates the columns of the record.
     * It retrieves the metadata for the table and gets the clustering key.
     * If the record contains a value for the clustering key, it uses binary search to find and delete the record.
     * If the record does not contain a value for the clustering key, it checks if there is an index on the table.
     * If there is an index, it uses the index to delete the record.
     * If there is no index, it iterates over the pages of the table and deletes the record.
     * Finally, it saves the table to the disk and updates the indexes of the table.
     *
     * @param strTableName     The name of the table.
     * @param htblColNameValue A Hashtable mapping column names to their values. This will be used in search to identify which rows/tuples to delete. Hashtable entries are ANDed together.
     * @throws DBAppException If the table name or the record is null, if an error occurs while deleting the record, or if an error occurs while updating the indexes.
     */
    public void deleteFromTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue) throws DBAppException {
        if (strTableName == null || htblColNameValue == null) {
            throw new DBAppException("Null arguments");
        }

        // delete all
        // delete all files in the table folder
        if (htblColNameValue.isEmpty()) {
            Table<Object> table = Table.loadTable(strTableName);
            File tableFolder = new File(getDbConfig().get("DataPath") + "/" + strTableName);
            File[] files = tableFolder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (!file.delete()) {
                        throw new DBAppException("Couldn't delete file: " + file.getName());
                    }
                }
            }
            table.clear();
            table.saveTable();

            return;
        }

        // 1. Validate the cols
        Util.validateCols(strTableName, htblColNameValue);

        // 2. Load the table & check if it exists
        Table<Object> table = Table.loadTable(strTableName);

        // 3. check if there is an index on the table
        Hashtable<String, Hashtable<String, String[]>> metaData = Util.getMetadata(strTableName);

        String pKey = metaData.get(strTableName).get("clusteringKey")[0];
        Object clusteringValue = htblColNameValue.get(pKey);

        // if the clustering key is in the delete condition just use binary search and delete
        if (clusteringValue != null) {
            int[] recordPos = Util.getRecordPos(strTableName, pKey, (Comparable) clusteringValue);
            if (recordPos[2] == 1) {
                Page page = table.getPage(recordPos[0]);
                Record record = page.getRecords().get(recordPos[1]);

                for (String colName : htblColNameValue.keySet()) {
                    if (!record.hashtable().get(colName).equals(htblColNameValue.get(colName))) {
                        return;
                    }
                }

                table.removeRecord(recordPos[1], pKey, page);
                Util.deleteIndexes(strTableName, recordPos[0], recordPos[1]);
                if (page.isEmpty()) {
                    table.removePage(page);
                    Util.recreateIndexes(strTableName, this);
                } else {
                    page.savePage();
                }
                table.saveTable();
            }
            return;
        }

        LinkedList<String> indexColumns = Util.getIndexColumns(metaData, strTableName);
        HashSet<String> indexColumsSet = new HashSet<>(indexColumns);
        indexColumsSet.retainAll(htblColNameValue.keySet());

        if (!indexColumns.isEmpty() && !indexColumsSet.isEmpty()) {
            //if there is an index
            deleteFromTableWithIndex(strTableName, htblColNameValue, indexColumns, table, metaData);
            return;
        }

        for (Page page : table.clone()) {
            deleteFromTableHelper(page, htblColNameValue, table);
        }

        table.saveTable(); //serialize the table
        Util.recreateIndexes(strTableName, this);
    }

    /**
     * This method is used to delete records from a table using an index.
     * It first checks if the table name, record, index columns, table, and metadata are not null.
     * It then iterates over the index columns and loads the index for each column.
     * It searches the index for the value of the column in the record and adds the result to a set.
     * If the set is empty, it adds all the results. If the set is not empty, it retains only the results that are also in the set.
     * It then converts the set to an array and clones the table.
     * It iterates over the pages in the array and deletes the records from the page.
     * Finally, it saves the table to the disk and recreates the indexes of the table.
     *
     * @param strTableName     The name of the table.
     * @param htblColNameValue A Hashtable mapping column names to their values. This will be used in search to identify which rows/tuples to delete. Hashtable entries are ANDed together.
     * @param indexColumns     A LinkedList containing the names of the index columns.
     * @param table            The table from which the records will be deleted.
     * @param metaData         A Hashtable containing the metadata of the table.
     * @throws DBAppException If the table name, record, index columns, table, or metadata is null, or if an error occurs while deleting the records or updating the indexes.
     */
    private void deleteFromTableWithIndex(String strTableName,
                                          Hashtable<String, Object> htblColNameValue,
                                          LinkedList<String> indexColumns,
                                          Table<Object> table,
                                          Hashtable<String, Hashtable<String, String[]>> metaData) throws DBAppException {
        // Set to store the result
        HashSet<Integer> result = new HashSet<>();

        for (String colName : indexColumns) {
            String indexName = metaData.get(strTableName).get(colName)[2];

            // 2. Load the index
            DBBTree BPlusTree = DBBTree.loadIndex(strTableName, indexName);
            HashSet<Integer> res = new HashSet<>();
            Object value = htblColNameValue.get(colName);

            if (value == null) continue;
            //search in the index for the value
            HashMap<Integer, Integer> search = BPlusTree.search((Comparable) value);
            if (search != null) {
                res.addAll(search.keySet());
            }

            if (result.isEmpty()) {
                result.addAll(res);
            } else {
                result.retainAll(res);
            }
        }

        Integer[] pages = result.toArray(new Integer[result.size()]);
        Table clonedTable = table.clone();
        // 5. Iterate over the pages to delete the records
        for (Integer integer : pages) {
            Page page = clonedTable.getPage(integer); // load the page from disk
            deleteFromTableHelper(page, htblColNameValue, table);
        }

        // 6. Update table metadata (optional)
        table.saveTable(); // serialize the table
        Util.recreateIndexes(strTableName, this);
    }

    /**
     * This helper method is used to delete records from a page in a table.
     * It first checks if the page, record, and table are not null.
     * It then creates a new vector to store the new records.
     * It iterates over the records in the page. For each record, it checks if the record should be deleted.
     * If the record should not be deleted, it adds the record to the new vector.
     * It then sets the records of the page to the new vector.
     * If the page is empty after the deletion, it removes the page from the table.
     * If the page is not empty, it saves the page to the disk.
     *
     * @param page             The page from which the records will be deleted.
     * @param htblColNameValue A Hashtable mapping column names to their values. This will be used in search to identify which rows/tuples to delete. Hashtable entries are ANDed together.
     * @param table            The table from which the records will be deleted.
     */
    private void deleteFromTableHelper(Page page, Hashtable<String, Object> htblColNameValue, Table table) {
        Vector<Record> newRecords = new Vector<>();
        // Iterate over the records in the page
        for (Record record : page.getRecords()) {
            boolean delete = true;
            // Loop over the columns in the record
            for (String colName : htblColNameValue.keySet()) {
                // If the record does not have the column or the value is not equal to the value in the condition
                if (!record.hashtable().get(colName).equals(htblColNameValue.get(colName))) {
                    delete = false;
                    break;
                }
            }
            if (!delete) {
                newRecords.add(record);
            }
        }
        page.setRecords(newRecords);
        // If the page is empty, remove it
        if (page.isEmpty()) {
            table.removePage(page);
        } else {
            page.savePage(); // Serialize the page
        }
    }

    /**
     * This method is used to select records from a table based on certain conditions.
     * It first checks if the SQL terms and operators are not null.
     * It then checks if the SQL terms and operators are valid.
     * It retrieves the table name from the first SQL term.
     * It then validates the operator and columns of each SQL term.
     * It loads the table from the disk and filters the pages using the index.
     * It then creates a new LinkedList to store the result.
     * It iterates over the filtered pages and selects the records from the page.
     * If the filtered pages are not empty, it returns an iterator for the result.
     * If the filtered pages are empty, it iterates over all the pages in the table and selects the records from the page.
     * Finally, it returns an iterator for the result.
     *
     * @param arrSQLTerms     An array of SQLTerm objects, each representing a condition in the SQL query.
     * @param strarrOperators An array of Strings, each representing an operator in the SQL query.
     * @return An Iterator for the result.
     * @throws DBAppException If the SQL terms or operators are null, if the SQL terms or operators are invalid, or if an error occurs while selecting the records.
     */
    public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
                                    String[] strarrOperators) throws DBAppException {
        if (arrSQLTerms == null || strarrOperators == null) {
            throw new DBAppException("Null arguments");
        }

        if (arrSQLTerms.length == 0 || strarrOperators.length == 0
                || arrSQLTerms.length != strarrOperators.length + 1) {
            throw new DBAppException("Invalid arguments");
        }

        String tableName = arrSQLTerms[0]._strTableName;

        for (SQLTerm term : arrSQLTerms) {
            if (!term._strOperator.equals("=") &&
                    !term._strOperator.equals("!=") &&
                    !term._strOperator.equals(">") &&
                    !term._strOperator.equals(">=") &&
                    !term._strOperator.equals("<") &&
                    !term._strOperator.equals("<=")
            ) {
                throw new DBAppException("Invalid operator");
            }

            Util.validateCols(tableName, new Hashtable<>(Map.of(term._strColumnName, term._objValue)));
        }

        Table<Object> table = Table.loadTable(tableName);
        HashSet<Integer> filteredPages = Util.filterPagesByIndex(arrSQLTerms, strarrOperators);
        LinkedList<Record> result = new LinkedList<>();

        for (Integer i : filteredPages) {
            for (Record record : table.getPage(i).getRecords()) {
                selectFromTableHelper(arrSQLTerms, strarrOperators, record, result);
            }
        }
        if (!filteredPages.isEmpty()) {
            return result.iterator();
        }

        for (Page p : table) {
            for (Record record : p.getRecords()) {
                selectFromTableHelper(arrSQLTerms, strarrOperators, record, result);
            }
        }
        return result.iterator();
    }

    /**
     * This helper method is used to select records from a table based on certain conditions.
     * It first checks if the SQL terms array has only one term.
     * If it does, it retrieves the value of the column in the record and evaluates the SQL term.
     * If the SQL term is true, it adds the record to the result.
     * If the SQL terms array has more than one term, it converts the SQL terms and operators to postfix notation.
     * It then evaluates the postfix expression.
     * If the postfix expression is true, it adds the record to the result.
     *
     * @param arrSQLTerms     An array of SQLTerm objects, each representing a condition in the SQL query.
     * @param strarrOperators An array of Strings, each representing an operator in the SQL query.
     * @param record          The record to be evaluated.
     * @param result          A LinkedList to store the selected records.
     */
    private void selectFromTableHelper(SQLTerm[] arrSQLTerms, String[] strarrOperators,
                                       Record record, LinkedList<Record> result) {
        if (arrSQLTerms.length == 1) {
            SQLTerm term = arrSQLTerms[0];
            Object value = record.hashtable().get(term._strColumnName);
            if (Util.evaluateSqlTerm((Comparable) value, term._strOperator, (Comparable) term._objValue)) {
                result.add(record);
            }

            return;
        }

        LinkedList<Object> postfix = Util.toPostfix(record.hashtable(), arrSQLTerms, strarrOperators);
        boolean res = Util.evaluatePostfix(postfix);
        if (res) {
            result.add(record);
        }
    }

    /**
     * This method is used to get the database configuration.
     * It first checks if the database configuration is null.
     * If it is, it throws a RuntimeException.
     * If it is not, it returns the database configuration.
     *
     * @return The database configuration.
     * @throws RuntimeException If the database configuration is null.
     */
    public static Properties getDbConfig() {
        if (db_config == null) {
            throw new RuntimeException("DBApp not initialized");
        }

        return db_config;
    }

    public static void main(String[] args) {
        try {
            String strTableName = "Student";
            DBApp dbApp = new DBApp();

            Hashtable htblColNameType = new Hashtable();
            htblColNameType.put("id", "java.lang.Integer");
            htblColNameType.put("name", "java.lang.String");
            htblColNameType.put("gpa", "java.lang.Double");
            dbApp.createTable(strTableName, "id", htblColNameType);
            dbApp.createIndex(strTableName, "gpa", "gpaIndex");

            Hashtable htblColNameValue = new Hashtable();
            htblColNameValue.put("id", Integer.valueOf(2343432));
            htblColNameValue.put("name", new String("Ahmed Noor"));
            htblColNameValue.put("gpa", Double.valueOf(0.95));
            dbApp.insertIntoTable(strTableName, htblColNameValue);

            htblColNameValue.clear();
            htblColNameValue.put("id", Integer.valueOf(453455));
            htblColNameValue.put("name", new String("Ahmed Noor"));
            htblColNameValue.put("gpa", Double.valueOf(0.95));
            dbApp.insertIntoTable(strTableName, htblColNameValue);

            htblColNameValue.clear();
            htblColNameValue.put("id", Integer.valueOf(5674567));
            htblColNameValue.put("name", new String("Dalia Noor"));
            htblColNameValue.put("gpa", Double.valueOf(1.25));
            dbApp.insertIntoTable(strTableName, htblColNameValue);

            htblColNameValue.clear();
            htblColNameValue.put("id", Integer.valueOf(23498));
            htblColNameValue.put("name", new String("John Noor"));
            htblColNameValue.put("gpa", Double.valueOf(1.5));
            dbApp.insertIntoTable(strTableName, htblColNameValue);

            htblColNameValue.clear();
            htblColNameValue.put("id", Integer.valueOf(78452));
            htblColNameValue.put("name", new String("Zaky Noor"));
            htblColNameValue.put("gpa", Double.valueOf(0.88));
            dbApp.insertIntoTable(strTableName, htblColNameValue);


            SQLTerm[] arrSQLTerms;
            arrSQLTerms = new SQLTerm[2];
            arrSQLTerms[0] = new SQLTerm();
            arrSQLTerms[0]._strTableName = "Student";
            arrSQLTerms[0]._strColumnName = "name";
            arrSQLTerms[0]._strOperator = "=";
            arrSQLTerms[0]._objValue = "John Noor";

            arrSQLTerms[1] = new SQLTerm();
            arrSQLTerms[1]._strTableName = "Student";
            arrSQLTerms[1]._strColumnName = "gpa";
            arrSQLTerms[1]._strOperator = "=";
            arrSQLTerms[1]._objValue = Double.valueOf(1.5);

            String[] strarrOperators = new String[1];
            strarrOperators[0] = "OR";
            // select * from Student where name = "John Noor" or gpa = 1.5;
            Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
        } catch (Exception exp) {
            exp.printStackTrace();
        }
    }
}