package DB;

import BTree.BTree;
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
            // print table before deleting
//            System.out.println("Printing table before deleting");
//            System.out.println(Table.loadTable(strTableName));

            Hashtable htblColNameValue = new Hashtable();
            htblColNameValue.put("gpa", 7.7);
            dbApp.deleteFromTable(strTableName, htblColNameValue);

            // Check that the rows have been deleted
            // print the table after delete
//            System.out.println("Printing table after deleting");
            Table table = Table.loadTable(strTableName);
//            System.out.println(table);

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
            String strTableName = "TestDeleteMultipleFromTableUsingIndex";
            DBApp dbApp = new DBApp();

            Hashtable htblColNameType = new Hashtable();
            htblColNameType.put("id", "java.lang.Integer");
            htblColNameType.put("name", "java.lang.String");
            htblColNameType.put("gpa", "java.lang.Double");
            dbApp.createTable(strTableName, "id", htblColNameType);

            // Insert multiple rows
            for (int i = 0; i < 10; i++) {
                Hashtable htblColNameValue = new Hashtable();
                htblColNameValue.put("id", Integer.valueOf(i));
                htblColNameValue.put("name", new String("Name " + i));
                htblColNameValue.put("gpa", Double.valueOf(i * 0.1));
                dbApp.insertIntoTable(strTableName, htblColNameValue);
            }

            // Create an index on the 'id' column
            dbApp.createIndex(strTableName, "id", "idIndex");


            //print table before deleting
            System.out.println(
                    "Printing table before deleting the first 5 rows using index"
            );
            System.out.println(Table.loadTable(strTableName).toString());

            //print index before deletion
            System.out.println(
                    "Printing index before deleting the first 5 rows using index"
            );
            System.out.println(DBBTree.loadIndex(strTableName, "idIndex").toString());

            // Delete multiple rows using the index
            for (int i = 0; i < 5; i++) {
                Hashtable htblColNameValue = new Hashtable();
                htblColNameValue.put("id", Integer.valueOf(i));
                dbApp.deleteFromTable(strTableName, htblColNameValue);
            }

            // Check that the rows have been deleted
            //print the table after delete
            System.out.println(
                    "Printing table after deleting the first 5 rows using index"
            );
            System.out.println(Table.loadTable(strTableName).toString());

            //print index after deletion
            System.out.println(
                    "Printing index after deleting the first 5 rows using index"
            );
            System.out.println(DBBTree.loadIndex(strTableName, "idIndex").toString());
        } catch (DBAppException e) {
            throw new RuntimeException(e);
        }
    }
}

