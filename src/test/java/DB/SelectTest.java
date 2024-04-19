package DB;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("ALL")
class SelectTest {

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
        assertFalse(result);
    }

    @org.junit.jupiter.api.Test
    void testGetRecordPos() {
        try {
            String strTableName = "TestGetRecordPos";
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
            fail("DBAppException thrown");
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
            htblColNameType.put("grade", "java.lang.Integer");
            dbApp.createTable(strTableName, "id", htblColNameType);
            dbApp.createIndex(strTableName, "gpa", "DAGPA");
            dbApp.createIndex(strTableName, "grade", "DAGrade");

            Hashtable htblColNameValue = new Hashtable();
            htblColNameValue.put("id", Integer.valueOf(23));
            htblColNameValue.put("name", new String("Ahmed Noor"));
            htblColNameValue.put("gpa", Double.valueOf(0.95));
            htblColNameValue.put("grade", Integer.valueOf(10));
            dbApp.insertIntoTable(strTableName, htblColNameValue);

            htblColNameValue.clear();
            htblColNameValue.put("id", Integer.valueOf(10));
            htblColNameValue.put("name", new String("Dalia Noor"));
            htblColNameValue.put("gpa", Double.valueOf(0.95));
            htblColNameValue.put("grade", Integer.valueOf(11));
            dbApp.insertIntoTable(strTableName, htblColNameValue);

            htblColNameValue.clear();
            htblColNameValue.put("id", Integer.valueOf(56));
            htblColNameValue.put("name", new String("John Noor"));
            htblColNameValue.put("gpa", Double.valueOf(1.5));
            htblColNameValue.put("grade", Integer.valueOf(11));
            dbApp.insertIntoTable(strTableName, htblColNameValue);

            htblColNameValue.clear();
            htblColNameValue.put("id", Integer.valueOf(2));
            htblColNameValue.put("name", new String("John Noor"));
            htblColNameValue.put("gpa", Double.valueOf(1.5));
            htblColNameValue.put("grade", Integer.valueOf(11));
            dbApp.insertIntoTable(strTableName, htblColNameValue);

            htblColNameValue.clear();
            htblColNameValue.put("id", Integer.valueOf(7));
            htblColNameValue.put("name", new String("Zaky Noor"));
            htblColNameValue.put("gpa", Double.valueOf(0.88));
            htblColNameValue.put("grade", Integer.valueOf(12));
            dbApp.insertIntoTable(strTableName, htblColNameValue);

            // name = "John Noor" OR gpa = 1.5
            SQLTerm[] arrSQLTerms;
            arrSQLTerms = new SQLTerm[3];

            arrSQLTerms[0] = new SQLTerm();
            arrSQLTerms[0]._strTableName = strTableName;
            arrSQLTerms[0]._strColumnName = "name";
            arrSQLTerms[0]._strOperator = ">=";
            arrSQLTerms[0]._objValue = "D";

            arrSQLTerms[1] = new SQLTerm();
            arrSQLTerms[1]._strTableName = strTableName;
            arrSQLTerms[1]._strColumnName = "gpa";
            arrSQLTerms[1]._strOperator = "<=";
            arrSQLTerms[1]._objValue = Double.valueOf(1.5);

            arrSQLTerms[2] = new SQLTerm();
            arrSQLTerms[2]._strTableName = strTableName;
            arrSQLTerms[2]._strColumnName = "grade";
            arrSQLTerms[2]._strOperator = "=";
            arrSQLTerms[2]._objValue = 11;

            String[] strarrOperators = new String[2];
            strarrOperators[0] = "AND";
            strarrOperators[1] = "AND";
            Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);

            ArrayList list = new ArrayList();
            resultSet.forEachRemaining(list::add);

            assertEquals(3, list.size());
            assertEquals(2, ((Record) list.get(0)).hashtable().get("id"));
            assertEquals(10, ((Record) list.get(1)).hashtable().get("id"));
            assertEquals(56, ((Record) list.get(2)).hashtable().get("id"));
        } catch (DBAppException e) {
            e.printStackTrace();
            fail("DBAppException thrown");
        }
    }
}