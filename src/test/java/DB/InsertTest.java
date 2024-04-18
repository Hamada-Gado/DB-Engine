package DB;

import java.util.Hashtable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class InsertTest {
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
//            System.out.println(Table.loadTable(strTableName));
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

//            System.out.println(table);
            assertEquals(3, table.pagesCount());
        } catch (DBAppException e) {
            e.printStackTrace();
            assertFalse(true);
        }
    }
}
