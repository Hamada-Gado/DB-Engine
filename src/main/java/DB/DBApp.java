package DB;

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
        File dataFolder = new File("src/main/resources/data");
        if (!dataFolder.exists()) {
            boolean newDir = dataFolder.mkdirs();
            if (!newDir) {
                throw new RuntimeException("Couldn't make data folder");
            }
        }

        // Create the metadata folder if it doesn't exist
        File metadataFile = new File(getDb_config().getProperty("MetadataPath"));
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

        String metadataPath = getDb_config().getProperty("MetadataPath");

        // create a new table, and parent folder
        Table table = new Table(strTableName);
        Path tablePath = Paths.get((String) getDb_config().get("DataPath"), strTableName);
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
        Path path = Paths.get((String) getDb_config().get("DataPath"), strTableName, strTableName + ".ser");
        try {
            FileOutputStream fileOut = new FileOutputStream(path.toAbsolutePath().toString());
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(table);
            out.close();
            fileOut.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    // following method creates a B+tree index
    public void createIndex(String strTableName,
                            String strColName,
                            String strIndexName) throws DBAppException {

        throw new DBAppException("not implemented yet");
    }


    // following method inserts one row only.
    // htblColNameValue must include a value for the primary key
    public void insertIntoTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue) throws DBAppException {

        throw new DBAppException("not implemented yet");
    }


    // following method updates one row only
    // htblColNameValue holds the key and new value
    // htblColNameValue will not include clustering key as column name
    // strClusteringKeyValue is the value to look for to find the row to update.
    public void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String, Object> htblColNameValue) throws DBAppException {

        throw new DBAppException("not implemented yet");
    }


    // following method could be used to delete one or more rows.
    // htblColNameValue holds the key and value. This will be used in search
    // to identify which rows/tuples to delete.
    // htblColNameValue enteries are ANDED together
    public void deleteFromTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue) throws DBAppException {

            //1. Load the table & check if it exists
            Table table = Table.loadTable(strTableName);

            //2. check if there is an index on the table
            Hashtable<String, Hashtable<String, String[]>> metaData = Util.getMetadata(strTableName);
                if (metaData == null) {
                   throw new DBAppException("Table not found");
            }
                ArrayList indexColumns = new ArrayList();
                //loop over metaData file and check if the index exists
                for (String colName : metaData.keySet()) {
                    Hashtable<String, String[]> colData = metaData.get(colName);
                    if (colData.get("IndexName") != null) {
                        indexColumns.add(colName);
                    }
                }

            // 3. Validate the delete condition
            if (htblColNameValue.isEmpty()) {
                throw new DBAppException("Delete condition cannot be empty.");
            }

            //4. Iterate through each page in the table to find the record to delete if no index
            if(indexColumns.size() == 0){
                outerloop:
                for (int i = 0; i < table.getPages().size(); i++) {
                    //load page from disk
                    Page page = table.getPage(i);
                    //iterate over the records in the page
                    for (int j = 0; j < page.getRecords().size(); j++) {
                        // get the record
                        Hashtable<String, Object> record = page.getRecords().get(j);
                        boolean delete = true;
                        //keyset is the columns in the record
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
                            page.getRecords().remove(j);

                            //if the page is empty, remove it
                            if (page.isEmpty()) {
                                table.getPages().remove(i);
                                table.updateTable(); //serialize the table
                            } else {
                                page.updatePage(); //serialize the page
                                table.updateTable(); //serialize the table
                            }
                            break outerloop;
                        }
                    }
                }
            }
            else{
                //if there is an index
                deleteFromTableHelper(strTableName, htblColNameValue, indexColumns, table, metaData);
            }
    }

    public void deleteFromTableHelper(String strTableName,
                                Hashtable<String, Object> htblColNameValue,
                                      ArrayList indexColumns,Table table,Hashtable<String, Hashtable<String, String[]>> metaData) throws DBAppException {
        Page p = null;
        int wantedPage =0;
        //loop over the index columns
        for(int i = 0; i < indexColumns.size(); i++){
            //get the index column
            String indexColumn = (String) indexColumns.get(i);
            //get the index name
            String indexName = metaData.get(indexColumn).get("IndexName")[0];
            //get the index type
            String indexType = metaData.get(indexColumn).get("IndexType")[0];
            //get the index file
            String indexFile = (String) getDb_config().get("DataPath") + "/" + strTableName + "/" + indexName + ".ser";
            //load the index
            bplustree index = Util.loadIndex(indexFile, indexType);
            //get the value of the index column in the condition
            Object value = htblColNameValue.get(indexColumn);
            //get the page number of the record
            int pageNumber = (int) index.search(value);
            wantedPage = pageNumber;
            //get Page using pageNumber
            p = table.getPage(pageNumber);

            //deleting from BPTree the value and updating BPTree
            index.delete(value);

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
            biggerloop:
            for (int j = 0; j < p.getRecords().size(); j++) {
                // get the record
                Hashtable<String, Object> record = p.getRecords().get(j);
                boolean delete = true;
                //keyset is the columns in the record
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
                        table.getPages().remove(wantedPage);
                        table.updateTable(); //serialize the table
                    } else {
                        p.updatePage(); //serialize the page
                        table.updateTable(); //serialize the table
                    }
                    break biggerloop;
                }
            }

//        //get the index column
//        String indexColumn = (String) indexColumns.get(0);
//        //get the index name
//        String indexName = metaData.get(indexColumn).get("IndexName")[0];
//        //get the index type
//        String indexType = metaData.get(indexColumn).get("IndexType")[0];
//        //get the index file
//        String indexFile = (String) getDb_config().get("DataPath") + "/" + strTableName + "/" + indexName + ".ser";
//        //load the index
//        bplustree index = Util.loadIndex(indexFile, indexType);
//        //get the value of the index column in the condition
//        Object value = htblColNameValue.get(indexColumn);
//        //get the page number of the record
//        int pageNumber = (int) index.search(value);
//        //deleteing from BPTree the value and updating BPTree
//        index.delete(value);
//        //load the page
//        Page page = table.getPage(pageNumber);
//        //iterate over the records in the page
//        biggerloop:
//        for (int j = 0; j < page.getRecords().size(); j++) {
//            // get the record
//            Hashtable<String, Object> record = page.getRecords().get(j);
//            boolean delete = true;
//            //keyset is the columns in the record
//            //loop over the columns in the record
//            for (String colName : htblColNameValue.keySet()) {
//                //if the record does not have the column or the value is not equal to the value in the condition
//                //get() gets the value of the column
//                if (!record.get(colName).equals(htblColNameValue.get(colName))) {
//                    delete = false;
//                    break;
//                }
//            }
//            if (delete) {
//                //remove the record
//                page.getRecords().remove(j);
//
//                //if the page is empty, remove it
//                if (page.isEmpty()) {
//                    table.getPages().remove(pageNumber);
//                    table.updateTable(); //serialize the table
//                } else {
//                    page.updatePage(); //serialize the page
//                    table.updateTable(); //serialize the table
//                }
//                break biggerloop;
//            }
        //}
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

            Util.validateTypes(term._strColumnName, new Hashtable<>(Map.of(term._strColumnName, term._objValue)));
        }

        String tableName = arrSQLTerms[0]._strTableName;
        LinkedList<Hashtable<String, Object>> result = new LinkedList<>();

        for (Page p : Table.loadTable(tableName)) {
            for (Hashtable<String, Object> record : p.getRecords()) {

                if (arrSQLTerms.length == 1) {
                    SQLTerm term = arrSQLTerms[0];
                    Object value = record.get(term._strColumnName);
                    if (Util.evaluateSqlTerm(value, term._strOperator, term._objValue)) {
                        result.add(record);
                    }
                    continue;
                }

                LinkedList<Object> postfix = Util.toPostfix(record, arrSQLTerms, strarrOperators);
                boolean res = Util.evaluatePostfix(postfix);
                if (res) {
                    result.add(record);
                }
            }
        }


        return result.iterator();
    }

    public static Properties getDb_config() {
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