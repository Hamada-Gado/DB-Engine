package DB;

import BTree.DBBTree;
import org.jetbrains.annotations.NotNull;

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

    // this does whatever initialization you would like
    // or leave it empty if there is no code you want to
    // execute at application startup
    public void init() {
        // Read the config file
        try (FileReader
                     reader = new FileReader(configPath)) {
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

    // following method creates one table only
    // strClusteringKeyColumn is the name of the column that will be the primary
    // key and the clustering column as well. The data type of that column will
    // be passed in htblColNameType
    // htblColNameValue will have the column name as key and the data
    // type as value
    // Example:
    // data/teacher/teacher.ser
    // data/student/student.ser
    // data/student/31234124.ser
    public void createTable(@NotNull String strTableName,
                            @NotNull String strClusteringKeyColumn,
                            @NotNull Hashtable<String, String> htblColNameType) throws DBAppException {

        for (String colName : htblColNameType.keySet()) {
            if (!htblColNameType.get(colName).equals("java.lang.Integer") &&
                    !htblColNameType.get(colName).equals("java.lang.Double") &&
                    !htblColNameType.get(colName).equals("java.lang.String")
            ) {
                throw new DBAppException("Invalid column type");
            }
        }

        String metadataPath = getDbConfig().getProperty("MetadataPath");

        // create a new table, and parent folder
        Table table = new Table(strTableName);
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


    // following method creates a B+tree index
    public void createIndex(String strTableName,
                            String strColName,
                            String strIndexName) throws DBAppException {
        if (strTableName == null || strColName == null || strIndexName == null) {
            throw new DBAppException("Null arguments");
        }

        // Load the table from the disk
        Table table = Table.loadTable(strTableName);

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

        // get metadata
        Hashtable<String, Hashtable<String, String[]>> metadata = Util.getMetadata(strTableName);
        Hashtable<String, String[]> columnData = metadata.get(strTableName);
        String[] columnDataArray = columnData.get(strColName);

        String metadataPath = getDbConfig().getProperty("MetadataPath");
        try (FileWriter writer = new FileWriter(metadataPath, true)) {
            writer.write(strTableName + "," + strColName + "," + columnDataArray[0] + "," + columnDataArray[1] + "," + strIndexName + ",B+tree\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // following method inserts one row only.
    // htblColNameValue must include a value for the primary key
    public void insertIntoTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue) throws DBAppException {
        if (strTableName == null || htblColNameValue == null){
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
        Comparable pValue = (Comparable) htblColNameValue.get(pKey);

        Table currentTable = Table.loadTable(strTableName);

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


    // following method updates one row only
    // htblColNameValue holds the key and new value
    // htblColNameValue will not include clustering key as column name
    // strClusteringKeyValue is the value to look for to find the row to update.
    public void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String, Object> htblColNameValue) throws DBAppException {
        //return null lw el clusKey msh mwgood
        if (strClusteringKeyValue == null) {
            throw new DBAppException("no clustering key is null");
        }

        Table table = Table.loadTable(strTableName);
        Hashtable<String, Hashtable<String, String[]>> metaData = Util.getMetadata(strTableName);

        //return null lw el table name msh mwgood
        if (metaData.get(strTableName) == null) {
            throw new DBAppException("Table does not exist");
        }

        for (String colName : htblColNameValue.keySet()) {

            String colType = metaData.get(strTableName).get(colName)[0];

            if (colType.equals("java.lang.Integer") && !(htblColNameValue.get(colName) instanceof Integer))
                throw new DBAppException("Mismatching dataTypes");

            else if (colType.equals("java.lang.String") && !(htblColNameValue.get(colName) instanceof String))
                throw new DBAppException("Mismatching dataTypes");

            else if (colType.equals("java.lang.Double") && !(htblColNameValue.get(colName) instanceof Double))
                throw new DBAppException("Mismatching dataTypes");


            int[] info = Util.getRecordPos(strTableName, strClusteringKeyValue, colName);
            if (info[2] == 0) {
                throw new DBAppException("Key Not found");
            }

            Page page = table.getPage(info[0]);
            Vector<Record> records = page.getRecords();
            Record record = records.get(info[1]);
            record.hashtable().forEach((key, value) -> {
                if (htblColNameValue.containsKey(key)) {
                    record.hashtable().put(key, htblColNameValue.get(key));
                }
            });
            records.set(info[1], record);
            page.updatePage();
        }
    }


    // following method could be used to delete one or more rows.
    // htblColNameValue holds the key and value. This will be used in search
    // to identify which rows/tuples to delete.
    // htblColNameValue enteries are ANDED together
    public void deleteFromTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue) throws DBAppException {

        // 1. Validate the delete condition
        if (htblColNameValue != null && htblColNameValue.isEmpty()) {
            throw new DBAppException("Delete condition cannot be empty.");
        }
        Util.validateCols(strTableName, htblColNameValue);

        // 2. Load the table & check if it exists
        Table table = Table.loadTable(strTableName);

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
                    page.updatePage();
                }
                table.updateTable();
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

        table.updateTable(); //serialize the table
        Util.recreateIndexes(strTableName, this);
    }


    private void deleteFromTableWithIndex(String strTableName,
                                          Hashtable<String, Object> htblColNameValue,
                                          LinkedList<String> indexColumns,
                                          Table table,
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
        table.updateTable(); // serialize the table
        Util.recreateIndexes(strTableName, this);
    }

    private void deleteFromTableHelper(Page page, Hashtable<String, Object> htblColNameValue, Table table) {
        Vector<Record> newRecords = new Vector<>();
        //iterate over the records in the page
        for (Record record : page.getRecords()) {
            boolean delete = true;
            //key-set is the columns in the record
            //loop over the columns in the record
            for (String colName : htblColNameValue.keySet()) {
                //if the record does not have the column or the value is not equal to the value in the condition
                //get() gets the value of the column
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
        //if the page is empty, remove it
        if (page.isEmpty()) {
            table.removePage(page);
        } else {
            page.updatePage(); //serialize the page
        }
    }

    // select * from student where name = "John Noor" OR gpa = 1.5 AND id = 2343432;
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

        Table table = Table.loadTable(tableName);
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

    public static Properties getDbConfig() {
        if (db_config == null) {
            throw new RuntimeException("DBApp not initialized");
        }

        return db_config;
    }

    public static void test() {
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
            arrSQLTerms[0]._strTableName = "Student";
            arrSQLTerms[0]._strColumnName = "name";
            arrSQLTerms[0]._strOperator = "=";
            arrSQLTerms[0]._objValue = "John Noor";

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

    public static void main(String[] args) {
        DBApp.test();
    }
}