package DB;

import BTree.DBBTree;

import java.util.HashMap;
import java.util.Hashtable;


import static org.junit.jupiter.api.Assertions.*;

public class DeleteTests {
    @org.junit.jupiter.api.Test
    void testDeleteMultipleFromTable() {
        try {
            String strTableName = "TestDeleteMultipleFromTable";
            DBApp dbApp = new DBApp();
            DBApp.getDbConfig().put("MaximumRowsCountinPage", "2");

            Hashtable htblColNameType = new Hashtable();
            htblColNameType.put("id", "java.lang.Integer");
            htblColNameType.put("name", "java.lang.String");
            htblColNameType.put("gpa", "java.lang.Double");
            dbApp.createTable(strTableName, "id", htblColNameType);
            dbApp.createIndex(strTableName, "name", "nameIndex");

            // Insert multiple rows
            for (int i = 0; i < 10; i++) {
                Hashtable htblColNameValue = new Hashtable();
                htblColNameValue.put("id", Integer.valueOf(i));
                htblColNameValue.put("name", new String("Name " + i));
                if (i == 1 || i % 3 == 0) {
                    htblColNameValue.put("gpa", Double.valueOf(7.7));
                    htblColNameValue.put("name", "winner");
                } else {
                    htblColNameValue.put("gpa", Double.valueOf(0.7));
                    htblColNameValue.put("name", "loser");
                }
                dbApp.insertIntoTable(strTableName, htblColNameValue);
            }

            Hashtable htblColNameValue = new Hashtable();
            htblColNameValue.put("gpa", 7.7);
            dbApp.deleteFromTable(strTableName, htblColNameValue);

            // Check that the rows have been deleted
            Table table = Table.loadTable(strTableName);

            // Check that the remaining rows still exist
            assertEquals(4, table.pagesCount());
            for (Page page : table) {
                for (Record record : page.getRecords()) {
                    if (record.hashtable().get("gpa").equals(7.7))
                        fail("Record with gpa = 7.7 still exists");
                }
            }

            // Check that the index has been updated
            DBBTree index = DBBTree.loadIndex(strTableName, "nameIndex");
            HashMap<Integer, Integer> res = index.search("winner");
            assertNull(res);

            res = index.search("loser");
            assertEquals(4, res.size());
            assertEquals(1, res.get(0));
            assertEquals(2, res.get(1));
            assertEquals(1, res.get(2));
            assertEquals(1, res.get(3));
        } catch (DBAppException e) {
            e.printStackTrace();
            fail("DBAppException thrown");
        }
    }

    @org.junit.jupiter.api.Test
    void testDeleteMultipleFromTableUsingIndex() {
        try {
            String strTableName = "TestDeleteMultipleFromTableWithIndex";
            DBApp dbApp = new DBApp();
            DBApp.getDbConfig().put("MaximumRowsCountinPage", "2");

            Hashtable htblColNameType = new Hashtable();
            htblColNameType.put("id", "java.lang.Integer");
            htblColNameType.put("name", "java.lang.String");
            htblColNameType.put("gpa", "java.lang.Double");
            dbApp.createTable(strTableName, "id", htblColNameType);
            dbApp.createIndex(strTableName, "name", "nameIndex");

            // Insert multiple rows
            for (int i = 0; i < 10; i++) {
                Hashtable htblColNameValue = new Hashtable();
                htblColNameValue.put("id", Integer.valueOf(i));
                htblColNameValue.put("name", new String("Name " + i));
                if (i == 1 || i % 3 == 0) {
                    htblColNameValue.put("gpa", Double.valueOf(7.7));
                    htblColNameValue.put("name", "winner");
                } else {
                    htblColNameValue.put("gpa", Double.valueOf(0.7));
                    htblColNameValue.put("name", "loser");
                }
                dbApp.insertIntoTable(strTableName, htblColNameValue);
            }

            dbApp.createIndex(strTableName, "gpa", "gpaIndex");

            Hashtable htblColNameValue = new Hashtable();
            htblColNameValue.put("gpa", 7.7);
            dbApp.deleteFromTable(strTableName, htblColNameValue);

            // Check that the rows have been deleted
            Table table = Table.loadTable(strTableName);

            // Check that the remaining rows still exist
            assertEquals(4, table.pagesCount());
            for (Page page : table) {
                for (Record record : page.getRecords()) {
                    if (record.hashtable().get("gpa").equals(7.7))
                        fail("Record with gpa = 7.7 still exists");
                }
            }

            // Check that the index has been updated
            DBBTree index = DBBTree.loadIndex(strTableName, "nameIndex");
            HashMap<Integer, Integer> res = index.search("winner");
            assertNull(res);

            res = index.search("loser");
            assertEquals(4, res.size());
            assertEquals(1, res.get(0));
            assertEquals(2, res.get(1));
            assertEquals(1, res.get(2));
            assertEquals(1, res.get(3));

            // Check that the index has been updated
            index = DBBTree.loadIndex(strTableName, "gpaIndex");
            res = index.search(7.7);
            assertNull(res);

            res = index.search(0.7);
            assertEquals(4, res.size());
            assertEquals(1, res.get(0));
            assertEquals(2, res.get(1));
            assertEquals(1, res.get(2));
            assertEquals(1, res.get(3));
        } catch (DBAppException e) {
            e.printStackTrace();
            fail("DBAppException thrown");
        }
    }


    @org.junit.jupiter.api.Test
    void testDeleteAll() {
        try {
            String strTableName = "TestDeleteAll";
            DBApp dbApp = new DBApp();
            DBApp.getDbConfig().put("MaximumRowsCountinPage", "2");

            Hashtable htblColNameType = new Hashtable();
            htblColNameType.put("id", "java.lang.Integer");
            htblColNameType.put("name", "java.lang.String");
            htblColNameType.put("gpa", "java.lang.Double");
            dbApp.createTable(strTableName, "id", htblColNameType);
            dbApp.createIndex(strTableName, "name", "nameIndex");

            // Insert multiple rows
            for (int i = 0; i < 10; i++) {
                Hashtable htblColNameValue = new Hashtable();
                htblColNameValue.put("id", Integer.valueOf(i));
                htblColNameValue.put("name", new String("Name " + i));
                if (i == 1 || i % 3 == 0) {
                    htblColNameValue.put("gpa", Double.valueOf(7.7));
                    htblColNameValue.put("name", "winner");
                } else {
                    htblColNameValue.put("gpa", Double.valueOf(0.7));
                    htblColNameValue.put("name", "loser");
                }
                dbApp.insertIntoTable(strTableName, htblColNameValue);
            }

            dbApp.deleteFromTable(strTableName, new Hashtable());

            // Check that the rows have been deleted
            Table table = Table.loadTable(strTableName);

            // Check that the remaining rows still exist
            assertEquals(0, table.pagesCount());
            assertTrue(table.getPagesPath().isEmpty());
            assertTrue(table.getClusteringKeyMin().isEmpty());

            // Check that the index has been updated
            try {
                DBBTree.loadIndex(strTableName, "nameIndex");
            } catch (DBAppException e) {
                assertEquals("Index nameIndex does not exist", e.getMessage());
            }

        } catch (DBAppException e) {
            e.printStackTrace();
            fail("DBAppException thrown");
        }
    }
}

