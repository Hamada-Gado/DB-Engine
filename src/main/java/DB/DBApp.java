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
        // Load the table from the disk
        Table table = Table.loadTable(strTableName);

        // Create a new B+ tree
        DBBTree bpt = new DBBTree(strTableName, strIndexName);

        // Iterate over all the records in the table
        for (int i = 0; i < table.pagesCount(); i++) {
            Page page = table.getPage(i);
            for (Hashtable<String, Object> record : page.getRecords()) {
                // Insert the value of the column and the record's key into the B+ tree
                bpt.insert((Comparable) record.get(strColName), i);
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
        //ToDo: validation
        //lw el insert mfhoosh values
        if  (htblColNameValue == null){
            throw new DBAppException(("No value being inserted"))
        }

        Hashtable<String, Hashtable<String, String[]>> metaData = Util.getMetadata(strTableName);

        //lw el insert fe data types 8alat
        int c = 1;
        for (String key : htblColNameValue.keySet()) {
            if (!(htblColNameValue.get(key).getClass().getName() == metaData.get(strTableName).get("Column Type")[c])) {
                throw new DBAppException("Invalid data type in new entry");
            }
            c++;
        }

        //lw el insert 3ndo columns msh nafs el 3adad zy el table
        if (!(htblColNameValue.size()==metaData.size())){
            throw new DBAppException("Invalid record being inserted");
        }


        if (metaData.get(strTableName) == null) {
            throw new DBAppException("Table not found");
        }

        String pKey = metaData.get(strTableName).get("clusteringKey")[0];
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
                currentTable.addRecord(recordNo + 1, htblColNameValue, pKey, page);
                Util.updateIndexes(strTableName, currentPageNo, recordNo + 1, metaData);
                if (page.size() == page.getMax() + 1 ) {
                    Util.deleteIndexes(strTableName, currentPageNo, page.getMax(), metaData);
                    htblColNameValue = currentTable.removeRecord(page.getMax(), pKey, page);
                    recordNo = 0;
                } else {
                    break;
                }
            } else {
                Page newPage = currentTable.addPage(Integer.parseInt((String) DBApp.getDbConfig().get("MaximumRowsCountinPage")));
                currentTable.addRecord(htblColNameValue, pKey, newPage);
                Util.updateIndexes(strTableName, currentPageNo, recordNo, metaData);
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
            Vector<Hashtable<String, Object>> records = page.getRecords();
            Hashtable<String, Object> record = records.get(info[1]);
            record.forEach((key, value) -> {
                if (htblColNameValue.containsKey(key)) {
                    record.put(key, htblColNameValue.get(key));
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
        //TODO: 1- handle index correctly, due to new format of btree

        // 1. Validate the delete condition
        if (htblColNameValue.isEmpty()) {
            throw new DBAppException("Delete condition cannot be empty.");
        }

        // 2. Load the table & check if it exists
        Table table = Table.loadTable(strTableName);

        // 3. check if there is an index on the table
        Hashtable<String, Hashtable<String, String[]>> metaData = Util.getMetadata(strTableName);
        if (metaData == null) {
            throw new DBAppException("Table not found");
        }

        String pKey = metaData.get(strTableName).get("clusteringKey")[0];

        ArrayList<String> indexColumns = new ArrayList<>();
        //loop over metaData file and check if the index exists
        for (String colName : metaData.keySet()) {
            // check if index name is not null in meta-data file
            if (!metaData.get(strTableName).get(colName)[2].equals("null")) {
                indexColumns.add(colName);
            }
        }

        //4. Iterate through each page in the table to find the record to delete if no index
        if (!indexColumns.isEmpty()) {
            //if there is an index
            deleteFromTableHelper(strTableName, htblColNameValue, indexColumns, table, metaData);
            return;
        }

        for (int i = 0; i < table.pagesCount(); i++) {
            //load page from disk
            Page page = table.getPage(i);
            //iterate over the records in the page
            for (int j = 0; j < page.getRecords().size(); j++) {
                // get the record
                Hashtable<String, Object> record = page.getRecords().get(j);
                boolean delete = true;
                //key-set is the columns in the record
                //loop over the columns in the record
                for (String colName : htblColNameValue.keySet()) {
                    //if the record does not have the column or the value is not equal to the value in the condition
                    //get() gets the value of the column

                    // !! what is the operation???? is it always equals?
                    if (!record.get(colName).equals(htblColNameValue.get(colName))) {
                        delete = false;
                        break;
                    }
                }
                if (delete) {
                    //remove the record
                    table.removeRecord(j, pKey, page);

                    //if the page is empty, remove it
                    if (page.isEmpty()) {
                        table.getPagesPath().remove(i);
                    } else {
                        page.updatePage(); //serialize the page
                    }
                }
            }
        }
        table.updateTable(); //serialize the table
    }

    public void deleteFromTableHelper(String strTableName,
                                      Hashtable<String, Object> htblColNameValue,
                                      ArrayList<String> indexColumns, Table table, Hashtable<String, Hashtable<String, String[]>> metaData) throws DBAppException {
        Page p = null;
        //loop over the index columns
        for (String column : indexColumns) {
            //get the index column
            String indexColumn = column;
            //get the index name
            String indexName = metaData.get(strTableName).get(column)[2];
            //load the index
            DBBTree index = DBBTree.loadIndex(strTableName, indexName);
            //get the value of the index column in the condition
            Object value = htblColNameValue.get(indexColumn);
            //get the page number of the record
            HashMap<Integer, Integer> pageNumbers = index.search((Integer) value);

//            if (pageNumber == null) {throw new DBAppException("wut da helllllllll");}


            //get Page
            p = table.getPage(pageNumbers.get(0)); //<--- not sure if this is correct

            //deleting from BPTree the value and updating BPTree
            index.delete((Integer) value);

            //save the index on disk
            Path indexPath = Paths.get((String) db_config.get("DataPath"), strTableName, indexName + ".ser");
            try {
                FileOutputStream fileOut = new FileOutputStream(indexPath.toAbsolutePath().toString());
                ObjectOutputStream out = new ObjectOutputStream(fileOut);
                out.writeObject(index);
                out.close();
                fileOut.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }


            //iterate over the records in the page
            for (int j = 0; j < p.getRecords().size(); j++) {
                // get the record
                Hashtable<String, Object> record = p.getRecords().get(j);
                boolean delete = true;
                //key-set is the columns in the record
                //loop over the columns in the record
                for (String colName : htblColNameValue.keySet()) {
                    //if the record does not have the column or the value is not equal to the value in the condition
                    //get() gets the value of the column
                    if (!record.get(colName).equals(htblColNameValue.get(colName))) {
                        delete = false;
                        break;
                    }
                }
                if (delete) {
                    //remove the record
                    p.getRecords().remove(j);

                    //if the page is empty, remove it
                    if (p.isEmpty()) {
                        table.getPagesPath().remove(pageNumbers.get(0));
                    } else {
                        p.updatePage(); //serialize the page
                    }
                }
            }
        }
        table.updateTable(); //serialize the table
    }

    // Following method returns a set of the pages of the select query using any index
    private HashSet<Integer> filterPagesByIndex(
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
                SQLTerm term = arrSQLTerms[i];
                String before = i == 0 ? null : strarrOperators[i - 1];
                String after = i == arrSQLTerms.length - 1 ? null : strarrOperators[i];

                if (term._strColumnName.equals(col)
                        && ((before != null && before.equals("AND"))
                        || (after != null && after.equals("AND")))) {

                    Object value = term._objValue;
                    HashMap<Integer, Integer> search = index.search((Comparable) value);
                    if (search != null) {
                        res.addAll(search.keySet());
                    }
                }
            }

            result.addAll(res);
        }

        return result;
    }

    private void selectFromTableHelper(SQLTerm[] arrSQLTerms, String[] strarrOperators,
                                       Hashtable<String, Object> record, LinkedList<Hashtable<String, Object>> result) {

        if (arrSQLTerms.length == 1) {
            SQLTerm term = arrSQLTerms[0];
            Object value = record.get(term._strColumnName);
            if (Util.evaluateSqlTerm((Comparable) value, term._strOperator, (Comparable) term._objValue)) {
                result.add(record);
            }

            return;
        }

        LinkedList<Object> postfix = Util.toPostfix(record, arrSQLTerms, strarrOperators);
        boolean res = Util.evaluatePostfix(postfix);
        if (res) {
            result.add(record);
        }
    }

    // select * from student where name = "John Noor" OR gpa = 1.5 AND id = 2343432;
    public Iterator selectFromTable(@NotNull SQLTerm[] arrSQLTerms,
                                    @NotNull String[] strarrOperators) throws DBAppException {

        if (arrSQLTerms.length == 0) {
            throw new DBAppException("No SQL terms provided");
        }

        if (arrSQLTerms.length != strarrOperators.length + 1) {
            throw new DBAppException("Invalid number of operators");
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

            Util.validateTypes(tableName, new Hashtable<>(Map.of(term._strColumnName, term._objValue)));
        }

        HashSet<Integer> filteredPages = filterPagesByIndex(arrSQLTerms, strarrOperators);
        LinkedList<Hashtable<String, Object>> result = new LinkedList<>();

        for (Page p : Table.loadTable(tableName)) {
            for (Hashtable<String, Object> record : p.getRecords()) {
                selectFromTableHelper(arrSQLTerms, strarrOperators, record, result);
            }
        }


        return result.iterator();
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