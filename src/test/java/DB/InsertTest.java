package DB;

import BTree.DBBTree;

import java.util.HashMap;
import java.util.Hashtable;

import static org.junit.jupiter.api.Assertions.*;

public class InsertTest {

    @org.junit.jupiter.api.Test
    void testInsertIntoTable() {
        try {
            String strTableName = "TestInsert";
            DBApp dbApp = new DBApp();

            Hashtable htblColNameType = new Hashtable();
            htblColNameType.put("id", "java.lang.Integer");
            htblColNameType.put("name", "java.lang.String");
            htblColNameType.put("gpa", "java.lang.Double");
            dbApp.createTable(strTableName, "id", htblColNameType);

            Hashtable htblColNameValue = new Hashtable();
            htblColNameValue.put("id", Integer.valueOf(20));
            htblColNameValue.put("name", new String("Ahmed Noor"));
            htblColNameValue.put("gpa", Double.valueOf(0.95));
            dbApp.insertIntoTable(strTableName, htblColNameValue);

            htblColNameValue.clear();
            htblColNameValue.put("id", Integer.valueOf(10));
            htblColNameValue.put("name", new String("Omar Noor"));
            htblColNameValue.put("gpa", Double.valueOf(0.95));
            dbApp.insertIntoTable(strTableName, htblColNameValue);

            htblColNameValue.clear();
            htblColNameValue.put("id", Integer.valueOf(50));
            htblColNameValue.put("name", new String("Dalia Noor"));
            htblColNameValue.put("gpa", Double.valueOf(1.25));
            dbApp.insertIntoTable(strTableName, htblColNameValue);

            htblColNameValue.clear();
            htblColNameValue.put("id", Integer.valueOf(30));
            htblColNameValue.put("name", new String("John Noor"));
            htblColNameValue.put("gpa", Double.valueOf(1.5));
            dbApp.insertIntoTable(strTableName, htblColNameValue);

            htblColNameValue.clear();
            htblColNameValue.put("id", Integer.valueOf(40));
            htblColNameValue.put("name", new String("Zaky Noor"));
            htblColNameValue.put("gpa", Double.valueOf(0.88));
            dbApp.insertIntoTable(strTableName, htblColNameValue);

            Table table = Table.loadTable(strTableName);
            assertEquals(1, table.pagesCount());
            Page page = table.getPage(0);
            int j;
            for (int i = j = 0; i < 5; i++) {
                j = (i + 1) * 10;
                assertEquals(j, page.getRecords().get(i).hashtable().get("id"));
            }
        } catch (DBAppException e) {
            e.printStackTrace();
            fail("DBAppException thrown");
        }
    }

    @org.junit.jupiter.api.Test
    void test5Inserts() {
        try {
            String strTableName = "Test5";
            DBApp dbApp = new DBApp();
            DBApp.getDbConfig().put("MaximumRowsCountinPage", "2");

            Hashtable htblColNameType = new Hashtable();
            htblColNameType.put("id", "java.lang.Integer");
            htblColNameType.put("name", "java.lang.String");
            htblColNameType.put("gpa", "java.lang.Double");
            dbApp.createTable(strTableName, "id", htblColNameType);

            Hashtable record = new Hashtable();
            record.put("name", "student");
            record.put("gpa", 5.0);
            for (int i = 0; i < 5; i++) {
                record.put("id", i);
                dbApp.insertIntoTable(strTableName, record);
            }

            assertEquals(3, Table.loadTable(strTableName).pagesCount());
        } catch (DBAppException e) {
            e.printStackTrace();
            fail("DBAppException thrown");
        }
    }

    @org.junit.jupiter.api.Test
    void test500Inserts() {
        try {
            long time = System.nanoTime();
            String strTableName = "Test500";
            DBApp dbApp = new DBApp();

            Hashtable htblColNameType = new Hashtable();
            htblColNameType.put("id", "java.lang.Integer");
            htblColNameType.put("name", "java.lang.String");
            htblColNameType.put("gpa", "java.lang.Double");
            dbApp.createTable(strTableName, "id", htblColNameType);
            dbApp.createIndex(strTableName, "name", "BTree-Name");

            Hashtable record = new Hashtable();
            for (int i = 0; i < 500; i++) {
                record.put("id", i);
                if (i < 200) {
                    record.put("name", "b");
                } else {
                    record.put("name", "a");
                }
                record.put("gpa", 0.5 + i);
                dbApp.insertIntoTable(strTableName, record);
            }

            time = (System.nanoTime() - time) / 1000000000;
            System.out.println("Time taken: " + time + " secs");
            Table table = Table.loadTable(strTableName);
            assertEquals(3, table.pagesCount());

            DBBTree index = DBBTree.loadIndex(strTableName, "BTree-Name");

            HashMap<Integer, Integer> res = index.search("a");
            assertNotNull(res);
            assertEquals(2, res.size());
            assertEquals(200, res.get(1));
            assertEquals(100, res.get(2));

            res = index.search("b");
            assertNotNull(res);
            assertEquals(1, res.size());
            assertEquals(200, res.get(0));
        } catch (DBAppException e) {
            e.printStackTrace();
            fail("DBAppException thrown");
        }
    }

    @org.junit.jupiter.api.Test
    void testWrongType() {
        try {
            String strTableName = "TestWrongType";
            DBApp dbApp = new DBApp();

            Hashtable htblColNameType = new Hashtable();
            htblColNameType.put("id", "java.lang.Integer");
            htblColNameType.put("name", "java.lang.String");
            htblColNameType.put("gpa", "java.lang.Double");
            dbApp.createTable(strTableName, "id", htblColNameType);

            Hashtable htblColNameValue = new Hashtable();
            htblColNameValue.put("id", Integer.valueOf(20));
            htblColNameValue.put("name", new String("Ahmed Noor"));
            htblColNameValue.put("gpa", Float.valueOf(0.95f));
            dbApp.insertIntoTable(strTableName, htblColNameValue);
        } catch (DBAppException e) {
            assertEquals("Invalid value for column gpa of type java.lang.Double", e.getMessage());
        }
    }
}
