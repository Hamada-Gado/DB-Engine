package DB;
/**
 * @author Wael Abouelsaadat
 */

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.stream.Collectors;


public class DBApp {

    static Properties db_config;

    public DBApp() {
        this.init();
    }

    // this does whatever initialization you would like
    // or leave it empty if there is no code you want to
    // execute at application startup
    public void init() {
        // Read the config file
        try (FileReader
                     reader = new FileReader("src/main/resources/DBApp.config")) {
            DBApp.db_config = new Properties();
            DBApp.db_config.load(reader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Create the data folder if it doesn't exist
        File file0 = new File("src/main/resources/data");
        if (!file0.exists()) {
            boolean newDir = file0.mkdirs();
            if (!newDir) {
                throw new RuntimeException("Couldn't make data folder");
            }
        }

        // Create the metadata folder if it doesn't exist
        File file1 = new File("src/main/resources/metadata.csv");
        if (!file1.exists()) {
            try {
                boolean newFile = file1.createNewFile();
                if (!newFile) {
                    throw new RuntimeException("Couldn't make metadata file");
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            // Add the metadata header
            try (FileWriter writer = new FileWriter(file1)) {
                writer.write("Table Name, Column Name, Column Type, ClusteringKey, IndexName, IndexType\n");
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
    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String, String> htblColNameType) throws DBAppException {
        // Example:
        // data/teacher/teacher.ser
        // data/student/student.ser
        // data/student/pages/31234124.ser

        // create a new table, and parent folder
        Table table = new Table(strTableName);

        // update metadata, and set clustering key

        // save table to disk
        Path path = Paths.get((String) DBApp.db_config.get("DataPath"), strTableName, strTableName + ".ser");
        try {
            FileOutputStream fileOut = new FileOutputStream(path.toAbsolutePath().toString());
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(table);
            out.close();
            fileOut.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        throw new DBAppException("not implemented yet");
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

        throw new DBAppException("not implemented yet");
    }


    public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
                                    String[] strarrOperators) throws DBAppException {

        return null;
    }

    public ArrayList<String[]> getMetadata() {
        ArrayList<String[]> metadata;
        String metadataPath = DBApp.db_config.getProperty("MetadataPath");

        try (BufferedReader br = new BufferedReader(new FileReader(metadataPath))) {
            metadata = br.lines().map(line -> line.split(",")).collect(Collectors.toCollection(ArrayList::new));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return metadata;
    }

    public static void test() {

        try {
            String strTableName = "Student";
            DBApp dbApp = new DBApp();

            Hashtable htblColNameType = new Hashtable();
            htblColNameType.put("id", "java.lang.Integer");
            htblColNameType.put("name", "java.lang.String");
            htblColNameType.put("gpa", "java.lang.double");
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