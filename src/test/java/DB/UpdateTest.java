package DB;

import BTree.DBBTree;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Hashtable;

import static org.junit.jupiter.api.Assertions.*;

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
            dbApp.createIndex(strTableName, "name", "nameIndex");

            // Update the record
            Hashtable<String, Object> htblColNameValueUpdate = new Hashtable<>();
            htblColNameValueUpdate.put("name", "Jane Doe");
            dbApp.updateTable(strTableName, "1", htblColNameValueUpdate);

            // Check if the record was updated
            Table table = Table.loadTable(strTableName);
            DBBTree tree = DBBTree.loadIndex(strTableName, "nameIndex");
            assertEquals("Jane Doe", table.getPage(0).getRecords().getFirst().hashtable().get("name"));
            HashMap<Integer, Integer> res = tree.search("Jane Doe");
            assertNotNull(res);
            assertEquals(1, res.size());
            assertTrue(res.containsKey(0));
            assertEquals(1, res.get(0));

            res = tree.search("John Doe");
            assertNull(res);
        } catch (DBAppException e) {
            e.printStackTrace();
            fail("An exception occurred");
        }
    }
}
