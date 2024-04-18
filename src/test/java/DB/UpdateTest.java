package DB;

import BTree.DBBTree;
import org.junit.jupiter.api.Test;

import java.util.Hashtable;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UpdateTest {

    @Test
    void testUpdateTable() {
        try {
            // Create an instance of DBApp
            DBApp dbApp = new DBApp();

            // Create a table
            String strTableName = "testUpdateTable";
            Hashtable<String, String> htblColNameType = new Hashtable<>();
            htblColNameType.put("id", "java.lang.Integer");
            htblColNameType.put("name", "java.lang.String");
            htblColNameType.put("gpa", "java.lang.Double");
            dbApp.createTable(strTableName, "id", htblColNameType);

            // Insert a record into the table
            Hashtable<String, Object> htblColNameValue = new Hashtable<>();
            htblColNameValue.put("id", 1);
            htblColNameValue.put("name", "John Doe");
            htblColNameValue.put("gpa", 3.5);
            dbApp.insertIntoTable(strTableName, htblColNameValue);
            System.out.println("Table created successfully");
            System.out.println(Table.loadTable(strTableName));

            //create index
            dbApp.createIndex(strTableName, "name","nameIndex");
            System.out.println("Index created successfully");
            DBBTree.loadIndex(strTableName,"nameIndex").print();
            // Update the record
            Hashtable<String, Object> htblColNameValueUpdate = new Hashtable<>();
            htblColNameValueUpdate.put("name", "Jane Doe");
            dbApp.updateTable(strTableName, "1", htblColNameValueUpdate);

            System.out.println("Record updated successfully");
            System.out.println(Table.loadTable(strTableName));
            System.out.println("index after update");
            DBBTree tree = DBBTree.loadIndex(strTableName,"nameIndex");
            System.out.println(tree);
            // Assert that the record was updated correctly
            // You will need to implement a method to retrieve a record from the table for this assertion
            // create index
        } catch (DBAppException e) {
            e.printStackTrace();

        }
    }
}
