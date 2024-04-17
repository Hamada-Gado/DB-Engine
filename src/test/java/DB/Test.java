package DB;

import BTree.DBBTree;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("ALL")
class Test {

    void naiveInsertIntoTable(String strTableName,
                              Hashtable<String, Object> htblColNameValue) throws DBAppException {
        naiveInsertIntoTable(strTableName, htblColNameValue, 0, 0);
    }

    void naiveInsertIntoTable(String strTableName,
                              Hashtable<String, Object> htblColNameValue, int pageNo, int recordNo) throws DBAppException {
        Hashtable<String, Hashtable<String, String[]>> metaData = Util.getMetadata(strTableName);
        if (metaData == null) {
            throw new DBAppException("Table not found");
        }

        String pKey = metaData.get(strTableName).get("clusteringKey")[0];
        Object pValue = htblColNameValue.get(pKey);

        Table currentTable = Table.loadTable(strTableName);

        for (int i = pageNo; i <= currentTable.pagesCount(); i++) {
            if (i < currentTable.pagesCount()) {
                Page page = currentTable.getPage(i);
                if (!currentTable.getPage(i).isFull()) {
                    currentTable.addRecord(recordNo, htblColNameValue, pKey, page);
                    break;
                } else {
                    currentTable.addRecord(recordNo, htblColNameValue, pKey, page);
                    htblColNameValue = currentTable.removeRecord(currentTable.getPage(i).getMax() - 1, pKey, page);
                    recordNo = 0;
                }
            } else {
                Page newPage = currentTable.addPage(Integer.parseInt((String) DBApp.getDbConfig().get("MaximumRowsCountinPage")));
                currentTable.addRecord(htblColNameValue, pKey, newPage);
                break;
            }
        }

        currentTable.updateTable();
    }


    @org.junit.jupiter.api.Test
    void testToPostfix2() {
        SQLTerm[] arrSQLTerms = new SQLTerm[2];
        arrSQLTerms[0] = new SQLTerm();
        arrSQLTerms[0]._strColumnName = "name";
        arrSQLTerms[0]._strOperator = "=";
        arrSQLTerms[0]._objValue = "John";
        arrSQLTerms[1] = new SQLTerm();
        arrSQLTerms[1]._strColumnName = "age";
        arrSQLTerms[1]._strOperator = "=";
        arrSQLTerms[1]._objValue = Integer.valueOf(20);

        String[] strarrOperators = new String[1];
        strarrOperators[0] = "AND";

        Hashtable record = new Hashtable();
        record.put("name", "John");
        record.put("age", Integer.valueOf(20));

        LinkedList postfix = Util.toPostfix(record, arrSQLTerms, strarrOperators);

        // name = "John" AND age = 20
        // [true, true, "AND"]
        // System.out.println(postfix);

        assertEquals(3, postfix.size());
        assertEquals(true, postfix.get(0));
        assertEquals(true, postfix.get(1));
        assertEquals("AND", postfix.get(2));
    }

    @org.junit.jupiter.api.Test
    void testToPostfix3() {

        SQLTerm[] arrSQLTerms = new SQLTerm[3];
        arrSQLTerms[0] = new SQLTerm();
        arrSQLTerms[0]._strColumnName = "name";
        arrSQLTerms[0]._strOperator = "=";
        arrSQLTerms[0]._objValue = "John";
        arrSQLTerms[1] = new SQLTerm();
        arrSQLTerms[1]._strColumnName = "age";
        arrSQLTerms[1]._strOperator = "=";
        arrSQLTerms[1]._objValue = Integer.valueOf(20);
        arrSQLTerms[2] = new SQLTerm();
        arrSQLTerms[2]._strColumnName = "gpa";
        arrSQLTerms[2]._strOperator = "=";
        arrSQLTerms[2]._objValue = Double.valueOf(3.5);

        String[] strarrOperators = new String[2];
        strarrOperators[0] = "OR";
        strarrOperators[1] = "AND";

        Hashtable record = new Hashtable();
        record.put("name", "John");
        record.put("age", Integer.valueOf(20));
        record.put("gpa", Double.valueOf(1.5));


        LinkedList postfix = Util.toPostfix(record, arrSQLTerms, strarrOperators);

        // name = "John" OR age = 20 AND gpa = 3.5
        // [true, true, false, "AND", "OR"]
        // System.out.println(postfix);

        assertEquals(5, postfix.size());
        assertEquals(true, postfix.get(0));
        assertEquals(true, postfix.get(1));
        assertEquals(false, postfix.get(2));
        assertEquals("AND", postfix.get(3));
        assertEquals("OR", postfix.get(4));
    }


    @org.junit.jupiter.api.Test
    void testToPostfix4() {

        SQLTerm[] arrSQLTerms = new SQLTerm[4];
        arrSQLTerms[0] = new SQLTerm();
        arrSQLTerms[0]._strColumnName = "name";
        arrSQLTerms[0]._strOperator = "=";
        arrSQLTerms[0]._objValue = "John";
        arrSQLTerms[1] = new SQLTerm();
        arrSQLTerms[1]._strColumnName = "age";
        arrSQLTerms[1]._strOperator = "=";
        arrSQLTerms[1]._objValue = Integer.valueOf(20);
        arrSQLTerms[2] = new SQLTerm();
        arrSQLTerms[2]._strColumnName = "gpa";
        arrSQLTerms[2]._strOperator = "=";
        arrSQLTerms[2]._objValue = Double.valueOf(3.5);
        arrSQLTerms[3] = new SQLTerm();
        arrSQLTerms[3]._strColumnName = "gender";
        arrSQLTerms[3]._strOperator = "=";
        arrSQLTerms[3]._objValue = "Male";


        String[] strarrOperators = new String[3];
        strarrOperators[0] = "OR";
        strarrOperators[1] = "AND";
        strarrOperators[2] = "XOR";

        Hashtable record = new Hashtable();
        record.put("name", "John");
        record.put("age", Integer.valueOf(20));
        record.put("gpa", Double.valueOf(1.5));
        record.put("gender", "Male");


        LinkedList postfix = Util.toPostfix(record, arrSQLTerms, strarrOperators);

        // name = "John" OR age = 20 AND gpa = 3.5 XOR gender = "Male"
        // [true, true, false, "AND", "OR", true, "XOR"]
        // System.out.println(postfix);

        assertEquals(7, postfix.size());
        assertEquals(true, postfix.get(0));
        assertEquals(true, postfix.get(1));
        assertEquals(false, postfix.get(2));
        assertEquals("AND", postfix.get(3));
        assertEquals("OR", postfix.get(4));
        assertEquals(true, postfix.get(5));
        assertEquals("XOR", postfix.get(6));
    }

    @org.junit.jupiter.api.Test
    void testEvaluatePostfix() {
        LinkedList postfix = new LinkedList();
        postfix.add(true);
        postfix.add(true);
        postfix.add("AND");

        boolean result = Util.evaluatePostfix(postfix);
        assertEquals(true, result);

        postfix.clear();
        postfix.add(true);
        postfix.add(true);
        postfix.add(false);
        postfix.add("AND");
        postfix.add("OR");

        result = Util.evaluatePostfix(postfix);
        assertEquals(true, result);

        postfix.clear();
        postfix.add(true);
        postfix.add(true);
        postfix.add(false);
        postfix.add("AND");
        postfix.add("OR");
        postfix.add(true);
        postfix.add("XOR");

        result = Util.evaluatePostfix(postfix);
        assertEquals(false, result);
    }

    void testNaiveInsertIntoTable(String strTableName) {
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

        try {
            Hashtable htblColNameValue = new Hashtable();
            htblColNameValue.put("id", Integer.valueOf(20));
            htblColNameValue.put("name", new String("Ahmed Noor"));
            htblColNameValue.put("gpa", Double.valueOf(0.95));
            naiveInsertIntoTable(strTableName, htblColNameValue, 0, 0);

            htblColNameValue.clear();
            htblColNameValue.put("id", Integer.valueOf(10));
            htblColNameValue.put("name", new String("Omar Noor"));
            htblColNameValue.put("gpa", Double.valueOf(0.95));
            naiveInsertIntoTable(strTableName, htblColNameValue, 0, 0);

            htblColNameValue.clear();
            htblColNameValue.put("id", Integer.valueOf(50));
            htblColNameValue.put("name", new String("Dalia Noor"));
            htblColNameValue.put("gpa", Double.valueOf(1.25));
            naiveInsertIntoTable(strTableName, htblColNameValue, 0, 2);

            htblColNameValue.clear();
            htblColNameValue.put("id", Integer.valueOf(30));
            htblColNameValue.put("name", new String("John Noor"));
            htblColNameValue.put("gpa", Double.valueOf(1.5));
            naiveInsertIntoTable(strTableName, htblColNameValue, 0, 2);

            htblColNameValue.clear();
            htblColNameValue.put("id", Integer.valueOf(40));
            htblColNameValue.put("name", new String("Zaky Noor"));
            htblColNameValue.put("gpa", Double.valueOf(0.88));
            naiveInsertIntoTable(strTableName, htblColNameValue, 0, 3);

            Table table = Table.loadTable(strTableName);
            assertEquals(1, table.pagesCount());
            Page page = table.getPage(0);
            int j;
            for (int i = j = 0; i < 5; i++) {
                j = (i + 1) * 10;
                assertEquals(j, page.getRecords().get(i).get("id"));
            }
        } catch (DBAppException e) {
            e.printStackTrace();
        }
    }

    @org.junit.jupiter.api.Test
    void testGetRecordPos() {
        String strTableName = "TestGetRecordPos";
        testNaiveInsertIntoTable(strTableName);

        try {
            int[] recordPos;
            recordPos = Util.getRecordPos(strTableName, "id", Integer.valueOf(20));
            assertArrayEquals(new int[]{0, 1, 1}, recordPos);

            recordPos = Util.getRecordPos(strTableName, "id", Integer.valueOf(21));
            assertArrayEquals(new int[]{0, 1, 0}, recordPos);

            recordPos = Util.getRecordPos(strTableName, "id", Integer.valueOf(45));
            assertArrayEquals(new int[]{0, 3, 0}, recordPos);

            recordPos = Util.getRecordPos(strTableName, "id", Integer.valueOf(50));
            assertArrayEquals(new int[]{0, 4, 1}, recordPos);
        } catch (DBAppException e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @org.junit.jupiter.api.Test
    void testSelectMethod() {
        try {
            String strTableName = "TestForSelectMethod";
            DBApp dbApp = new DBApp();
            DBApp.getDbConfig().put("MaximumRowsCountinPage", "2");

            Hashtable htblColNameType = new Hashtable();
            htblColNameType.put("id", "java.lang.Integer");
            htblColNameType.put("name", "java.lang.String");
            htblColNameType.put("gpa", "java.lang.Double");
            dbApp.createTable(strTableName, "id", htblColNameType);
            dbApp.createIndex(strTableName, "gpa", "DAGPA");

            Hashtable htblColNameValue = new Hashtable();
            htblColNameValue.put("id", Integer.valueOf(23));
            htblColNameValue.put("name", new String("Ahmed Noor"));
            htblColNameValue.put("gpa", Double.valueOf(0.95));
            dbApp.insertIntoTable(strTableName, htblColNameValue);

            htblColNameValue.clear();
            htblColNameValue.put("id", Integer.valueOf(10));
            htblColNameValue.put("name", new String("Dalia Noor"));
            htblColNameValue.put("gpa", Double.valueOf(0.95));
            dbApp.insertIntoTable(strTableName, htblColNameValue);

            htblColNameValue.clear();
            htblColNameValue.put("id", Integer.valueOf(56));
            htblColNameValue.put("name", new String("John Noor"));
            htblColNameValue.put("gpa", Double.valueOf(1.5));
            dbApp.insertIntoTable(strTableName, htblColNameValue);

            htblColNameValue.clear();
            htblColNameValue.put("id", Integer.valueOf(2));
            htblColNameValue.put("name", new String("John Noor"));
            htblColNameValue.put("gpa", Double.valueOf(1.5));
            dbApp.insertIntoTable(strTableName, htblColNameValue);

            htblColNameValue.clear();
            htblColNameValue.put("id", Integer.valueOf(7));
            htblColNameValue.put("name", new String("Zaky Noor"));
            htblColNameValue.put("gpa", Double.valueOf(0.88));
            dbApp.insertIntoTable(strTableName, htblColNameValue);

            // name = "John Noor" OR gpa = 1.5
            SQLTerm[] arrSQLTerms;
            arrSQLTerms = new SQLTerm[2];
            arrSQLTerms[0] = new SQLTerm();
            arrSQLTerms[0]._strTableName = strTableName;
            arrSQLTerms[0]._strColumnName = "name";
            arrSQLTerms[0]._strOperator = "=";
            arrSQLTerms[0]._objValue = "John Noor";

            arrSQLTerms[1] = new SQLTerm();
            arrSQLTerms[1]._strTableName = strTableName;
            arrSQLTerms[1]._strColumnName = "gpa";
            arrSQLTerms[1]._strOperator = "=";
            arrSQLTerms[1]._objValue = Double.valueOf(1.5);

            String[] strarrOperators = new String[1];
            strarrOperators[0] = "AND";
            Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);

            ArrayList list = new ArrayList();
            resultSet.forEachRemaining(list::add);

//            System.out.println(Table.loadTable(strTableName));
//            System.out.println(list);

            assertEquals(2, ((Hashtable) list.get(0)).get("id"));
            assertEquals(56, ((Hashtable) list.get(1)).get("id"));
        } catch (DBAppException e) {
            e.printStackTrace();
            assertTrue(false);
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
            System.out.println(Table.loadTable(strTableName));
        } catch (DBAppException e) {
            e.printStackTrace();
            assertFalse(true);
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

            Hashtable record = new Hashtable();
            record.put("gpa", 5.0);
            record.put("name", "student");
            for (int i = 0; i < 500; i++) {
                record.put("id", i);
                dbApp.insertIntoTable(strTableName, record);
            }

            time = (System.nanoTime() - time) / 1000000000;
            System.out.println("Time taken: " + time + " secs");
            Table table = Table.loadTable(strTableName);

            System.out.println(table);
            assertEquals(3, table.pagesCount());
        } catch (DBAppException e) {
            e.printStackTrace();
            assertFalse(true);
        }
    }

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
}