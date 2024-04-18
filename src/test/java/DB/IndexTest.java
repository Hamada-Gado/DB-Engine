package DB;

import BTree.BTree;
import BTree.DBBTree;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;

import static org.junit.jupiter.api.Assertions.*;

public class IndexTest {
    @org.junit.jupiter.api.Test
    void testIndex() {
        String strTableName = "TestIndex";
        try {
            DBApp dbApp = new DBApp();
            DBApp.getDbConfig().put("MaximumRowsCountinPage", "1");

            Hashtable htblColNameType = new Hashtable();
            htblColNameType.put("id", "java.lang.Integer");
            htblColNameType.put("name", "java.lang.String");
            htblColNameType.put("gpa", "java.lang.Double");
            dbApp.createTable(strTableName, "id", htblColNameType);
            dbApp.createIndex(strTableName, "name", "BTree-Name");

            Hashtable htblColNameValue = new Hashtable();
            htblColNameValue.put("id", Integer.valueOf(2343432));
            htblColNameValue.put("name", new String("Ahmed Noor"));
            htblColNameValue.put("gpa", Double.valueOf(0.95));
            dbApp.insertIntoTable(strTableName, htblColNameValue);

            htblColNameValue.clear();
            htblColNameValue.put("id", Integer.valueOf(5674567));
            htblColNameValue.put("name", new String("Dalia Noor"));
            htblColNameValue.put("gpa", Double.valueOf(1.5));
            dbApp.insertIntoTable(strTableName, htblColNameValue);
            dbApp.createIndex(strTableName, "gpa", "BTree-GPA");

            DBBTree<String> nameBTree = DBBTree.loadIndex(strTableName, "BTree-Name");
            DBBTree<Double> gpaBTree = DBBTree.loadIndex(strTableName, "BTree-GPA");

//            System.out.println(Table.loadTable(strTableName));
//            nameBTree.print();
//            gpaBTree.print();

            HashMap result = nameBTree.search("Ahmed Noor");
            assertTrue(result.keySet().contains(0));
            assertEquals(1, result.size());
            assertEquals(1, result.get(0));

            result = nameBTree.search("Dalia Noor");
            assertTrue(result.keySet().contains(1));
            assertEquals(1, result.size());
            assertEquals(1, result.get(1));

            result = gpaBTree.search(0.95);
            assertTrue(result.keySet().contains(0));
            assertEquals(1, result.size());
            assertEquals(1, result.get(0));

            result = gpaBTree.search(1.5);
            assertTrue(result.keySet().contains(1));
            assertEquals(1, result.size());
            assertEquals(1, result.get(1));
        } catch (DBAppException e) {
            e.printStackTrace();
            assertFalse(true);
        }
    }

    @org.junit.jupiter.api.Test
    void testRangeBTreeQuery() {
        BTree<String, Integer> bTree = new BTree<String, Integer>();

        bTree.insert("A", 1);
        bTree.insert("B", 2);
        bTree.insert("C", 3);
        bTree.insert("E", 4);
        bTree.insert("F", 5);
//        bTree.print();

        LinkedList<Integer> result = bTree.search("B", "E");
        assertEquals(3, result.size());
        assertEquals(2, result.get(0));
        assertEquals(3, result.get(1));
        assertEquals(4, result.get(2));

        result = bTree.search("E", null);
        assertEquals(2, result.size());
        assertEquals(4, result.get(0));
        assertEquals(5, result.get(1));

        result = bTree.search(null, "D");
        assertEquals(3, result.size());
        assertEquals(1, result.get(0));
        assertEquals(2, result.get(1));
        assertEquals(3, result.get(2));

        result = bTree.search(null, null);
        assertEquals(5, result.size());
        for (int i = 0; i < 5; i++) {
            assertEquals(i + 1, result.get(i));
        }
    }

    @org.junit.jupiter.api.Test
    void testRangeDBBTreeQuery() {
        String folderName = "src/main/resources/data/TestRangeDBBTreeQuery";
        File folder = new File(folderName);
        folder.mkdirs();

        new DBApp();
        DBBTree<String> bTree = new DBBTree<String>("TestRangeDBBTreeQuery", "TestRangeDBBTreeQuery");

        bTree.insert("A", 1);
        bTree.insert("B", 2);
        bTree.insert("B", 2);
        bTree.insert("D", 3);
        bTree.insert("E", 2);
        bTree.insert("E", 4);
        bTree.insert("F", 5);
//        bTree.print();

        HashSet<Integer> result = bTree.searchRange("B", "D");
        assertEquals(2, result.size());
        assertTrue(result.contains(2));
        assertTrue(result.contains(3));

        result = bTree.searchRange("C", null);
        assertEquals(4, result.size());
        assertTrue(result.contains(2));
        assertTrue(result.contains(3));
        assertTrue(result.contains(4));
        assertTrue(result.contains(5));

        result = bTree.searchRange(null, "C");
        assertEquals(2, result.size());
        assertTrue(result.contains(1));
        assertTrue(result.contains(2));

        result = bTree.searchRange(null, null);
        assertEquals(5, result.size());
        for (int i = 0; i < 5; i++) {
            assertTrue(result.contains(i + 1));
        }
    }
}
