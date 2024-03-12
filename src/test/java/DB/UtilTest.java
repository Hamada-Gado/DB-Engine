package DB;

import java.util.Hashtable;
import java.util.LinkedList;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("ALL")
class UtilTest {
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
        System.out.println(postfix);

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
        System.out.println(postfix);

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
        System.out.println(postfix);

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

        postfix = new LinkedList();
        postfix.add(true);
        postfix.add(true);
        postfix.add(false);
        postfix.add("AND");
        postfix.add("OR");

        result = Util.evaluatePostfix(postfix);
        assertEquals(true, result);

        postfix = new LinkedList();
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
}