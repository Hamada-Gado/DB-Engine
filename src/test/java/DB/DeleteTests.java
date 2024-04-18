package DB;

import BTree.DBBTree;

import java.util.Hashtable;


import static org.junit.jupiter.api.Assertions.*;

public class DeleteTests {
    @org.junit.jupiter.api.Test
    void testDeleteMultipleFromTable() {
        String strTableName = "Test";
        DBApp dbApp = new DBApp();

        Hashtable htblColNameType = new Hashtable();
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.Double");
        try {
            dbApp.createTable(strTableName, "id", htblColNameType);
        } catch (DBAppException e) {
            e.printStackTrace();
            assertTrue(false);
        }

        // Insert multiple rows
        for (int i = 0; i < 10; i++) {
            Hashtable htblColNameValue = new Hashtable();
            htblColNameValue.put("id", Integer.valueOf(i));
            htblColNameValue.put("name", new String("Name " + i));
            htblColNameValue.put("gpa", Double.valueOf(i * 0.1));
            try {
                dbApp.insertIntoTable(strTableName, htblColNameValue);
            } catch (DBAppException e) {
                e.printStackTrace();
                assertTrue(false);
            }
        }
        //print table before deleting
        System.out.println(
                "Printing table before deleting the first 5 rows"
        );
        try {
            System.out.println(Table.loadTable(strTableName).toString());
        } catch (DBAppException e) {
            throw new RuntimeException(e);
        }

        // Delete multiple rows
        for (int i = 0; i < 10; i++) {
            Hashtable htblColNameValue = new Hashtable();
            htblColNameValue.put("id", Integer.valueOf(i));
            try {
                dbApp.deleteFromTable(strTableName, htblColNameValue);
            } catch (DBAppException e) {
                e.printStackTrace();
                assertTrue(false);
            }
        }

        // Check that the rows have been deleted
        //print the table after delete
        System.out.println(
                "Printing table after deleting the first 5 rows"
        );
        try {
            System.out.println(Table.loadTable(strTableName).toString());
        } catch (DBAppException e) {
            throw new RuntimeException(e);
        }


        // Check that the remaining rows still exist
        for (int i = 5; i < 10; i++) {
            try {
                int[] recordPos = Util.getRecordPos(strTableName, "id", Integer.valueOf(i));
                assertNotEquals(new int[]{-1, -1, -1}, recordPos);
            } catch (DBAppException e) {
                e.printStackTrace();
                assertTrue(false);
            }
        }
    }

    @org.junit.jupiter.api.Test
    void testDeleteMultipleFromTableUsingIndex() {
        String strTableName = "Test";
        DBApp dbApp = new DBApp();

        Hashtable htblColNameType = new Hashtable();
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.Double");
        try {
            dbApp.createTable(strTableName, "id", htblColNameType);
        } catch (DBAppException e) {
            e.printStackTrace();
            assertTrue(false);
        }

        // Insert multiple rows
        for (int i = 0; i < 10; i++) {
            Hashtable htblColNameValue = new Hashtable();
            htblColNameValue.put("id", Integer.valueOf(i));
            htblColNameValue.put("name", new String("Name " + i));
            htblColNameValue.put("gpa", Double.valueOf(i * 0.1));
            try {
                dbApp.insertIntoTable(strTableName, htblColNameValue);
            } catch (DBAppException e) {
                e.printStackTrace();
                assertTrue(false);
            }
        }

        // Create an index on the 'id' column
        try {
            dbApp.createIndex(strTableName, "id", "idIndex");
        } catch (DBAppException e) {
            e.printStackTrace();
            assertTrue(false);
        }


        //print table before deleting
        System.out.println(
                "Printing table before deleting the first 5 rows using index"
        );
        try {
            System.out.println(Table.loadTable(strTableName).toString());
        } catch (DBAppException e) {
            throw new RuntimeException(e);
        }

        //print index before deletion
        System.out.println(
                "Printing index before deleting the first 5 rows using index"
        );
        try {
            System.out.println(DBBTree.loadIndex(strTableName, "idIndex").toString());
        } catch (DBAppException e) {
            throw new RuntimeException(e);
        }

        // Delete multiple rows using the index
        for (int i = 0; i < 5; i++) {
            Hashtable htblColNameValue = new Hashtable();
            htblColNameValue.put("id", Integer.valueOf(i));
            try {
                dbApp.deleteFromTable(strTableName, htblColNameValue);
            } catch (DBAppException e) {
                e.printStackTrace();
                assertTrue(false);
            }
        }

        // Check that the rows have been deleted
        //print the table after delete
        System.out.println(
                "Printing table after deleting the first 5 rows using index"
        );
        try {
            System.out.println(Table.loadTable(strTableName).toString());
        } catch (DBAppException e) {
            throw new RuntimeException(e);
        }

        //print index after deletion
        System.out.println(
                "Printing index after deleting the first 5 rows using index"
        );
        try {
            System.out.println(DBBTree.loadIndex(strTableName, "idIndex").toString());
        } catch (DBAppException e) {
            throw new RuntimeException(e);
        }
    }
}

